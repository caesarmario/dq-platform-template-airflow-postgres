# TMT Data Quality Platform

This repository contains a modular data quality platform designed to support the expansion of TakeMeThere (TMT), a ride-hailing company scaling to over 200K daily transactions. The solution covers three core data quality capabilities—**Observability**, **Monitoring**, and **Alerting**—and is built around the principles outlined in DAMA's data quality dimensions: **Accuracy**, **Completeness**, **Consistency**, **Uniqueness**, **Timeliness**, and **Validity**.

---

## 🔧 Tech Stack

- **Airflow** (Orchestration)
- **PostgreSQL** (Staging + Logging layer)
- **Python** (Custom logic)
- **Metabase** (Dashboard visualization)
- **MySQL / BigQuery** *(mocked as data source)*
- **Slack** *(mocked as alert target)*

---

## 🚀 Features

- ✅ **Observability**: SLA-based freshness & completeness checks with support for SKIPPED logic on historical tables.
- ✅ **Monitoring**: Declarative rule validation using SQL + dynamic schema casting.
- ✅ **Alerting**: Grouped Slack-style alerts for high/critical violations (mocked; pluggable).
- ✅ **Metabase Dashboard**: Visual overview of DQ metrics, trends, and alerts.
- ✅ **Extensible Design**: Add new rules/tables by modifying configs only.

---

## 🛠 Getting Started (Local)
1. Clone this repo
2. Run via Docker Compose:  
   ```bash
   docker-compose up --build
   ```
3. - Access Airflow: `http://localhost:8084`
    - Access Metabase: `http://localhost:3000`
4. Don't forget to **create all the schema and required tables** in the db
4. Trigger `dag_dq_load_sqlite_csv_to_postgres` to load source data
5. Monitor DQ results via `dag_data_quality_platform`

## 📈 Dashboards
Metabase dashboards include:
- Freshness SLA Status
- Completeness Check Table
- Top Rule Violations (7d)
- Violation Trend
- Live Unsent Alerts

## 📄 Notes
- BigQuery and MySQL are mocked with PostgreSQL for local dev.
- Slack alerting uses dummy webhook endpoint for test safety.
- Environment variables & connections should be customized for production use.
