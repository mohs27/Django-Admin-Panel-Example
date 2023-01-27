import os
import sqlite3

import psycopg2
from dotenv import load_dotenv
from postgres_saver import PostgresSaver
from psycopg2.extensions import connection as _connection
from psycopg2.extras import DictCursor
from sqlite_extractor import SQLiteExtractor

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

PARENT_DIR = os.path.split(os.path.abspath(BASE_DIR))[0]

dotenv_path = os.path.join(PARENT_DIR+'\\config\\', '.env')

load_dotenv(dotenv_path)


def load_from_sqlite(connection: sqlite3.Connection, pg_conn: _connection):
    """Основной метод загрузки данных из SQLite в Postgres."""
    tables_sqlite = [
        'film_work', 'genre', 'person',
        'genre_film_work', 'person_film_work',
    ]
    sqlite_extractor = SQLiteExtractor(connection)
    postgres_saver = PostgresSaver(pg_conn)
    for table in tables_sqlite:
        data = sqlite_extractor.get_data_from_sqlite(table)
        if table == 'film_work':
            postgres_saver.save_film_work_to_postgres(data)
        elif table == 'genre':
            postgres_saver.save_genre_to_postgres(data)
        elif table == 'person':
            postgres_saver.save_person_to_postgres(data)
        elif table == 'genre_film_work':
            postgres_saver.save_genre_film_work_to_postgres(data)
        elif table == 'person_film_work':
            postgres_saver.save_person_film_work_to_postgres(data)


if __name__ == '__main__':
    dsl = {'dbname': 'movies_database', 'user': 'app', 'password': '123qwe', 'host': '127.0.0.1', 'port': 5432}
    with sqlite3.connect('db.sqlite') as sqlite_conn, psycopg2.connect(**dsl, cursor_factory=DictCursor) as pg_conn:
        load_from_sqlite(sqlite_conn, pg_conn)
