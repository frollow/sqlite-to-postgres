# Data Migration Project to PostgreSQL

## Project Overview

This project focuses on migrating data into a PostgreSQL database using the psycopg2 and sqlite3 libraries. The primary goal is to seamlessly transfer data from SQLite to PostgreSQL, ensuring data integrity, avoiding duplicate records, and efficiently managing database schemas and operations.

## Contents

### 1. **movies_database.ddl**

This DDL file contains SQL commands necessary to construct the tables within the "content" schema in a PostgreSQL database. The schema includes tables for film works, genres, persons, and their relationships, structured with primary keys based on uuid. It includes:

**Tables** :

* `film_work`: Contains details of the film works like title, description, and ratings.
* `genre`: Lists different genres.
* `person`: Stores information about individuals involved in film works.
* `person_film_work`: Relationships between persons and film works with specific roles.
* `genre_film_work`: Relationships between genres and film works.

**Indexes** :

* Creation dates for film works.
* Unique indexes for film work-person-role combinations and film work-genre combinations to ensure data uniqueness and integrity.

### 2. **load_data.py**

A Python script responsible for migrating data from SQLite to PostgreSQL. It uses data classes to structure the database models and manage data integrity and transaction safety.

**Key Features**:

* Utilizes data classes for clear and efficient data modeling.
* Data is batch-loaded in chunks to optimize performance and manage large datasets.
* Implements checks to prevent duplicate entries on re-runs.
* Includes error handling for both read and write operations to ensure robustness.
* Accompanied by unit tests to verify the functionalities.

**Data Classes**:

* `Genre`, `Person`, `Filmwork`, `GenreFilmwork`, `PersonFilmwork`: These classes model the database tables and include fields like ids (uuid), creation, and modification timestamps.

**Database Operations**:

* Data is transferred from SQLite using batch operations to manage memory and connection resources efficiently.
* PostgreSQL transactions are managed through context managers to ensure data consistency and handle errors gracefully.
* Includes a method to adapt SQLite data keys to PostgreSQL columns during the migration process.

**Logging**:

* Configured to log detailed operational information which assists in monitoring the script’s execution and troubleshooting.

## Usage

To use this script, ensure that PostgreSQL and SQLite databases are correctly set up and accessible. Adjust the environment variables as necessary to match your database credentials and paths.

```
python load_data.py
```

This command initiates the migration process, reading from the SQLite database and writing to the PostgreSQL database, leveraging the structured data classes and handling all operations safely and efficiently.

## Conclusion

This project provides a robust solution for migrating data between different database systems, ensuring data integrity, performance, and ease of use with detailed logging and error handling.
