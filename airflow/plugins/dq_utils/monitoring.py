####
## Utils for monitoring purposes
## Tech Implementation Answer by Mario Caesar // caesarmario87@gmail.com
####

# -- Import libraries
import psycopg2
import json
import os
from datetime import datetime

from logging_utils import logger

# -- Config
PG_CONFIG = {
    'host': 'host.docker.internal',
    'port': 5436,
    'user': 'airflow',
    'password': 'airflow',
    'dbname': 'airflow'
}

# -- Functions
def cast_column_in_rule(rule_sql, schema_dict):
    for col, dtype in schema_dict.items():
        if dtype in ("REAL", "INTEGER"):
            rule_sql = rule_sql.replace(f"{col} ", f"CAST({col} AS FLOAT) ")
            rule_sql = rule_sql.replace(f"{col})", f"CAST({col} AS FLOAT))")
    return rule_sql

def run_dq_rules():
    config_path = "./config/dq_schema_config.json"
    with open(config_path, "r") as f:
        schema_config = json.load(f)

    conn = psycopg2.connect(**PG_CONFIG)
    cursor = conn.cursor()

    # Create dq_violations table if not exists
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS tmt_dq.dq_violations (
            rule_id TEXT,
            rule_name TEXT,
            table_name TEXT,
            rule_sql TEXT,
            severity TEXT,
            violation_count INTEGER,
            check_time TIMESTAMP,
            alert_sent BOOLEAN DEFAULT FALSE
        );
    """)

    # Read dq_rule_config json
    cursor.execute("""
        SELECT rule_id, rule_name, table_name, rule_sql, severity
        FROM tmt_dq.dq_rule_config;
    """)
    rules = cursor.fetchall()

    for rule_id, rule_name, table_name, rule_sql, severity in rules:
        try:
            if table_name in schema_config:
                rule_sql = cast_column_in_rule(rule_sql, schema_config[table_name])

            cursor.execute(rule_sql)
            count = cursor.fetchone()[0]

            if count > 0:
                logger.info(f"[!] Rule violated: {rule_name} ({count} rows)")
                cursor.execute("""
                    INSERT INTO tmt_dq.dq_violations (
                        rule_id, rule_name, table_name, rule_sql, severity,
                        violation_count, check_time
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, (
                    rule_id, rule_name, table_name, rule_sql, severity,
                    count, datetime.utcnow()
                ))
                conn.commit()
            else:
                logger.info(f"[✓] Passed: {rule_name}")
        except Exception as e:
            logger.error(f"[✗] Error executing rule {rule_name}: {e}")
            continue

    cursor.close()
    conn.close()
