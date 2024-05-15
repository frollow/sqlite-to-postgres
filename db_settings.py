from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

dsl = {
    "dbname": os.environ.get("DB_NAME"),
    "user": os.environ.get("DB_USER"),
    "password": os.environ.get("DB_PASSWORD"),
    "host": "localhost",
    "port": 5432,
    "options": os.environ.get("DB_OPTIONS"),
}
sql_db_path = os.environ.get("DB_PATH")