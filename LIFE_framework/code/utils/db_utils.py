import psycopg2
import pandas as pd
from psycopg2 import sql
from code.logger_config import get_logger

logger = get_logger()

def connect_postgres(config):
    return psycopg2.connect(
        dbname=config["dbname"],
        user=config["user"],
        password=config["password"],
        host=config["host"],
        port=config["port"]
    )

def run_query(conn, query):
    try:
        return pd.read_sql_query(query, conn)
    except Exception as e:
        logger.error(f"Failed to execute query: {e}")
        raise

def insert_dataframe(conn, df, table, schema):
    cursor = conn.cursor()
    for _, row in df.iterrows():
        columns = list(row.index)
        values = tuple(row)
        insert_query = sql.SQL("INSERT INTO {}.{} ({}) VALUES ({})").format(
            sql.Identifier(schema),
            sql.Identifier(table),
            sql.SQL(', ').join(map(sql.Identifier, columns)),
            sql.SQL(', ').join(sql.Placeholder() * len(values))
        )
        cursor.execute(insert_query, values)
    conn.commit()
    cursor.close()