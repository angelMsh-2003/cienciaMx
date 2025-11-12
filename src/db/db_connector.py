# db_connector.py
import os
import psycopg2
from psycopg2.extensions import connection as PgConnection
from dotenv import load_dotenv

# Carga las variables del archivo .env
load_dotenv()

DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAMES")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")


def get_db_connection() -> PgConnection:
    """
    Connects to PostgreSQL database
    """
    try:
        print(f"Connected to PostgreSQL {DB_HOST}:{DB_PORT}... Database: {DB_NAME}")
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASS
        )
        return conn
    except Exception as e:
        print(f"Error: Can not connect to the database {DB_NAME}.")
        print(e)
        exit(1)


if __name__ == "__main__":
    # Prueba rápida de conexión
    conn = get_db_connection()
    if conn:
        print("Connected!")
        conn.close()