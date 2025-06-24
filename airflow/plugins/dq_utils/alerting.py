####
## Utils for alerting purposes
## Tech Implementation Answer by Mario Caesar // caesarmario87@gmail.com
####

# -- Import libraries
import os
import requests
import psycopg2
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

SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL", "https://hooks.slack.com/services/XXXX")

# -- Function to send alert
def send_alerts_to_slack():
    try:
        conn = psycopg2.connect(**PG_CONFIG)
        cursor = conn.cursor()

        cursor.execute("""
            SELECT table_name, rule_name, severity, violation_count
            FROM tmt_dq.dq_violations
            WHERE severity IN ('high', 'critical') AND alert_sent = FALSE
            ORDER BY table_name, severity DESC;
        """)
        rows = cursor.fetchall()

        if not rows:
            logger.info("[✓] No high-severity violations found.")
            cursor.close()
            conn.close()
            return

        grouped_alerts = {}
        for row in rows:
            table, rule, severity, count = row
            if table not in grouped_alerts:
                grouped_alerts[table] = []
            grouped_alerts[table].append(f"🔸 *{rule}* ({severity.upper()}): `{count}` records")

        alert_time = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
        message = f"🚨 *Data Quality Alert ({alert_time})*\n"
        for table, violations in grouped_alerts.items():
            message += f"\n📦 *Table: `{table}`*\n" + "\n".join(violations) + "\n"

        # Slack alert
        try:
            resp = requests.post(SLACK_WEBHOOK_URL, json={"text": message})
            if resp.status_code == 200:
                logger.info("[✓] Alert sent to Slack.")
            else:
                logger.info(f"[!] Failed to send alert: {resp.status_code} {resp.text}")
        except Exception as e:
            logger.info(f"[!] Slack alert skipped: {e}")

        cursor.execute("""
            UPDATE tmt_dq.dq_violations
            SET alert_sent = TRUE
            WHERE severity IN ('high', 'critical') AND alert_sent = FALSE
        """)
        conn.commit()
        cursor.close()
        conn.close()

    except Exception as e:
        logger.error(f"[!] Alert task skipped due to error: {e}")