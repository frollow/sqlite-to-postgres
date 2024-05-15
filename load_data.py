import logging
import os
import sqlite3
import uuid
from contextlib import contextmanager
from dataclasses import astuple, dataclass, field, fields
from datetime import date, datetime

import psycopg2
from dotenv import load_dotenv
from psycopg2.extensions import connection as _connection
from psycopg2.extras import DictCursor, execute_values

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


# Data classes for database models
@dataclass
class Genre:
    name: str
    description: str = ""
    id: uuid.UUID = field(default_factory=uuid.uuid4)
    created: datetime = field(default_factory=datetime.now())
    modified: datetime = field(default_factory=datetime.now())


@dataclass
class Person:
    full_name: str
    id: uuid.UUID = field(default_factory=uuid.uuid4)
    created: datetime = field(default_factory=datetime.now())
    modified: datetime = field(default_factory=datetime.now())


@dataclass
class Filmwork:
    title: str
    description: str = ""
    rating: float = field(default=0.0)
    type: str = "MOV"
    certificate: str = ""
    file_path: str = ""
    creation_date: date = None
    id: uuid.UUID = field(default_factory=uuid.uuid4)
    created: datetime = field(default_factory=datetime.now())
    modified: datetime = field(default_factory=datetime.now())


@dataclass
class GenreFilmwork:
    id: uuid.UUID = field(default_factory=uuid.uuid4)
    film_work_id: uuid.UUID = field(default_factory=uuid.uuid4)
    genre_id: uuid.UUID = field(default_factory=uuid.uuid4)
    created: datetime = field(default_factory=datetime.now())


@dataclass
class PersonFilmwork:
    role: str
    id: uuid.UUID = field(default_factory=uuid.uuid4)
    film_work_id: uuid.UUID = field(default_factory=uuid.uuid4)
    person_id: uuid.UUID = field(default_factory=uuid.uuid4)
    created: datetime = field(default_factory=datetime.now())


@contextmanager
def sqlite_conn_context(db_path: str):
    """Context manager for SQLite connection"""
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
    finally:
        conn.close()


@contextmanager
def postgres_conn_context(dsl):
    """Context manager for PostgreSQL connection"""
    conn = psycopg2.connect(**dsl, cursor_factory=DictCursor)
    try:
        yield conn
    except psycopg2.Error as e:
        conn.rollback()
        raise e
    else:
        conn.commit()
    finally:
        conn.close()


def adapt_keys_for_postgres(row, column_names):
    """Function to adapt keys for PostgreSQL insertion"""
    row["created"] = row.pop("created_at", None)
    if "modified" in column_names:
        row["modified"] = row.pop("updated_at", None)
    return row


def load_from_sqlite(
    sqlite_conn: sqlite3.Connection,
    pg_conn: _connection,
    sqlite_table: str,
    postgres_table: str,
    data_class,
):
    """Function to load data from SQLite and save to PostgreSQL"""
    chunk_size = 4

    try:
        pg_cursor = pg_conn.cursor()
        sqlite_cursor = sqlite_conn.cursor()
        pg_cursor.execute(f"TRUNCATE content.{postgres_table}")
        column_names = [field.name for field in fields(data_class)]
        column_names_str = ", ".join(column_names)
        sqlite_cursor.execute(f"SELECT * FROM {sqlite_table};")
        while records := sqlite_cursor.fetchmany(chunk_size):
            for row in records:
                pg_records = [
                    astuple(
                        data_class(
                            **adapt_keys_for_postgres(dict(record), column_names)
                        )
                    )
                    for record in records
                ]
            execute_values(
                pg_cursor,
                f"INSERT INTO content.{postgres_table} ({column_names_str}) VALUES %s ON CONFLICT (id) DO NOTHING",
                pg_records,
            )
            pg_conn.commit()
    except (sqlite3.Error, psycopg2.Error) as e:
        logging.error(f"Error transferring data: {e}")
    finally:
        pg_cursor.close()
        sqlite_cursor.close()
        logging.info(
            f"Transfer completed from SQLite ({sqlite_table}) to PostgreSQL ({postgres_table})"
        )


if __name__ == "__main__":
    dsl = {
        "dbname": os.environ.get("DB_NAME"),
        "user": os.environ.get("DB_USER"),
        "password": os.environ.get("DB_PASSWORD"),
        "host": "localhost",
        "port": 5432,
        "options": os.environ.get("DB_OPTIONS"),
    }
    sql_db_path = os.environ.get("DB_PATH")

    with sqlite_conn_context(sql_db_path) as sqlite_conn, postgres_conn_context(
        dsl
    ) as pg_conn:
        for db_name, data_class in [
            ("film_work", Filmwork),
            ("genre", Genre),
            ("genre_film_work", GenreFilmwork),
            ("person", Person),
            ("person_film_work", PersonFilmwork),
        ]:
            load_from_sqlite(
                sqlite_conn=sqlite_conn,
                pg_conn=pg_conn,
                sqlite_table=db_name,
                postgres_table=db_name,
                data_class=data_class,
            )
