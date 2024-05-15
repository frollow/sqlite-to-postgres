import os
import sqlite3
import unittest

import psycopg2
from dotenv import load_dotenv
from psycopg2.extras import DictCursor

# Load environment variables
load_dotenv()


class TestDataConsistency(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.sqlite_db = os.environ.get("DB_PATH")
        cls.postgres_dsn = {
            "dbname": os.environ.get("DB_NAME"),
            "user": os.environ.get("DB_USER"),
            "password": os.environ.get("DB_PASSWORD"),
            "host": "localhost",
            "port": 5432,
        }
        cls.sqlite_conn = sqlite3.connect(cls.sqlite_db)
        cls.sqlite_conn.row_factory = sqlite3.Row

    def check_table_consistency(self, table_name):
        # Connect to SQLite
        sqlite_conn = sqlite3.connect(self.sqlite_db)
        sqlite_conn.row_factory = sqlite3.Row
        sqlite_cursor = self.sqlite_conn.cursor()
        sqlite_cursor.execute(f"SELECT * FROM {table_name}")
        sqlite_data = sqlite_cursor.fetchall()

        # Connect to PostgreSQL
        pg_conn = psycopg2.connect(**self.postgres_dsn, cursor_factory=DictCursor)
        pg_cursor = pg_conn.cursor()
        pg_cursor.execute(f"SELECT * FROM content.{table_name}")
        pg_data = pg_cursor.fetchall()

        # Compare the number of entries
        self.assertEqual(
            len(sqlite_data),
            len(pg_data),
            f"Record count mismatch in table {table_name}",
        )
        sqlite_conn.close()
        pg_conn.close()

    def test_genre_table_consistency(self):
        self.check_table_consistency("genre")

    def test_film_work_table_consistency(self):
        self.check_table_consistency("film_work")

    def test_person_table_consistency(self):
        self.check_table_consistency("person")

    def test_genre_film_work_table_consistency(self):
        self.check_table_consistency("genre_film_work")

    def test_person_film_work_table_consistency(self):
        self.check_table_consistency("person_film_work")


if __name__ == "__main__":
    unittest.main()
