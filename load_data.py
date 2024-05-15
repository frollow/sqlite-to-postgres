import sqlite3
from dataclasses import astuple, fields

import psycopg2
from context_managers import postgres_conn_context, sqlite_conn_context
from data_classes import Filmwork, Genre, GenreFilmwork, Person, PersonFilmwork
from db_settings import dsl, sql_db_path
from logger import logging
from psycopg2.extensions import connection as _connection
from psycopg2.extras import execute_values


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
