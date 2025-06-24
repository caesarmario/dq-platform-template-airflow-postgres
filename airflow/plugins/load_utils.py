import pandas as pd
import sqlite3
import psycopg2
from io import StringIO

def load_sqlite_to_postgres(sqlite_path, pg_config, table_name, sqlite_query):
    # Load data from SQLite
    conn_sqlite = sqlite3.connect(sqlite_path)
    df = pd.read_sql_query(sqlite_query, conn_sqlite)
    conn_sqlite.close()

    # Upload to PostgreSQL
    copy_dataframe_to_postgres(df, pg_config, table_name)
    print(f"[✓] Uploaded SQLite table '{table_name}' to PostgreSQL.")

def load_csv_to_postgres(csv_path, pg_config, table_name):
    df = pd.read_csv(csv_path)
    copy_dataframe_to_postgres(df, pg_config, table_name)
    print(f"[✓] Uploaded CSV file '{csv_path}' to PostgreSQL as table '{table_name}'.")

def copy_dataframe_to_postgres(df, pg_config, table_name):
    """
    Uploads a Pandas DataFrame to PostgreSQL using COPY FROM for efficiency.
    """  
    conn = psycopg2.connect(**pg_config)
    cursor = conn.cursor()

    # Optional: recreate the table
    cols = ', '.join([f'"{col}" TEXT' for col in df.columns]) 
    cursor.execute(f'DROP TABLE IF EXISTS tmt."{table_name}"')
    cursor.execute(f'CREATE TABLE tmt."{table_name}" ({cols})')

    # Convert DataFrame to CSV buffer
    buffer = StringIO()
    df.to_csv(buffer, index=False, header=False)
    buffer.seek(0)

    # Use COPY FROM
    cursor.copy_expert(f'COPY tmt."{table_name}" FROM STDIN WITH CSV', buffer)

    conn.commit()
    cursor.close()
    conn.close()
