####
## Utils for observability purposes
## Tech Implementation Answer by Mario Caesar // caesarmario87@gmail.com
####

# -- Import libraries
import psycopg2
from datetime import datetime, timedelta

# -- Config
PG_CONFIG = {
    'host': 'host.docker.internal',
    'port': 5436,
    'user': 'airflow',
    'password': 'airflow',
    'dbname': 'airflow'
}

FRESHNESS_TABLES = [
    {
        "table": "customer_transactions",
        "column": "transaction_date",
        "sla_minutes": 60,
        "expected_data_start": "2024-01-01"
    },
    {
        "table": "orders",
        "column": "order_purchase_timestamp",
        "sla_minutes": 60,
        "expected_data_start": "2022-01-01"
    },
    {
        "table": "order_reviews",
        "column": "review_creation_date",
        "sla_minutes": 1440,
        "expected_data_start": "2022-01-01"
    }
]


COMPLETENESS_TABLES = [
    {"table": "customer_transactions", "min_rows": 200000},
    {"table": "orders", "min_rows": 100000},
]

# -- Python functions
def check_freshness():
    conn = psycopg2.connect(**PG_CONFIG)
    cursor = conn.cursor()

    for t in FRESHNESS_TABLES:
        table = t["table"]
        column = t["column"]
        sla = timedelta(minutes=t["sla_minutes"])

        query = f'SELECT MAX("{column}") FROM tmt."{table}"'
        cursor.execute(query)
        result = cursor.fetchone()[0]

        now = datetime.utcnow()
        status = "PASS"
        delay_minutes = None

        expected_start = datetime.strptime(t.get("expected_data_start"), "%Y-%m-%d")
        if isinstance(result, datetime):
            data_time = result
        else:
            try:
                data_time = datetime.strptime(result, "%Y-%m-%d %H:%M:%S.%f")
            except ValueError:
                data_time = datetime.strptime(result, "%Y-%m-%d %H:%M:%S")

        delay = now - data_time
        delay_minutes = delay.total_seconds() / 60

        # New logic: skip if outdated
        if data_time < expected_start:
            status = "SKIPPED"
        else:
            status = "FAIL" if delay > sla else "PASS"

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS tmt_dq.dq_freshness_log (
                table_name TEXT, column_name TEXT, last_updated TIMESTAMP,
                check_time TIMESTAMP, status TEXT, delay_minutes REAL
            );
        """)
        cursor.execute("""
            INSERT INTO tmt_dq.dq_freshness_log
            (table_name, column_name, last_updated, check_time, status, delay_minutes)
            VALUES (%s, %s, %s, %s, %s, %s);
        """, (table, column, result, now, status, delay_minutes))
        conn.commit()

    cursor.close()
    conn.close()


def check_completeness():
    conn = psycopg2.connect(**PG_CONFIG)
    cursor = conn.cursor()

    for t in COMPLETENESS_TABLES:
        table = t["table"]
        expected = t["min_rows"]

        cursor.execute(f'SELECT COUNT(*) FROM tmt."{table}"')
        actual = cursor.fetchone()[0]
        status = "PASS" if actual >= expected else "FAIL"

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS tmt_dq.dq_completeness_log (
                table_name TEXT, row_count INTEGER, expected_min_rows INTEGER,
                check_time TIMESTAMP, status TEXT
            );
        """)
        cursor.execute("""
            INSERT INTO tmt_dq.dq_completeness_log
            (table_name, row_count, expected_min_rows, check_time, status)
            VALUES (%s, %s, %s, %s, %s);
        """, (table, actual, expected, datetime.utcnow(), status))
        conn.commit()

    cursor.close()
    conn.close()
