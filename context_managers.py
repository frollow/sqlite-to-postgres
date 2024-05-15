import sqlite3
from contextlib import contextmanager

import psycopg2
from psycopg2.extras import DictCursor


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
