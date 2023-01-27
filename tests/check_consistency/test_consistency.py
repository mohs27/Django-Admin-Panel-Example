
import datetime
import sqlite3

import psycopg2
from dateutil import parser
from psycopg2.extras import DictCursor

from sqlite_to_postgres.postgres_saver import PostgresSaver
from sqlite_to_postgres.sqlite_extractor import SQLiteExtractor


def connect_to_databases(table_sqlite, table_pg):
    """Соединение с постргресом и скулайт для получения всех данных."""
    dsl = {'dbname': 'movies_database', 'user': 'app', 'password': '123qwe', 'host': '127.0.0.1', 'port': 5432}
    with sqlite3.connect('sqlite_to_postgres/db.sqlite') as sqlite_conn, \
            psycopg2.connect(**dsl, cursor_factory=DictCursor) as pg_conn:
        sqlite_extractor = SQLiteExtractor(sqlite_conn)
        postgres_saver = PostgresSaver(pg_conn)

        data_sq = sqlite_extractor.get_data_from_sqlite(table_sqlite)

        data_from_pg = postgres_saver.get_data_from_postgres(table_pg)

        return data_sq, data_from_pg


def test_records_count():
    """Тестирование соответствия количества записей при переносе данных из SQLite в PostgreSQL."""
    tables_sqlite = [
        'genre', 'person', 'film_work',
        'genre_film_work', 'person_film_work',
    ]
    for table in tables_sqlite:
        data_sq, data_from_pg = connect_to_databases(table_sqlite=table, table_pg='content.'+table)
        assert len(data_sq) == len(data_from_pg)


def test_consistency():
    """Тестирование соответствия содержимого записей при переносе данных из SQLite в PostgreSQL."""
    tables_sqlite = [
        'genre', 'person', 'film_work',
        'genre_film_work', 'person_film_work',
    ]
    for table in tables_sqlite:
        data_sq, data_from_pg = connect_to_databases(table_sqlite=table, table_pg='content.'+table)

        for record in range(0, len(data_sq)):
            for field in range(0, len(data_sq[record])):
                if not isinstance(data_from_pg[record][field], datetime.date):
                    assert str(data_sq[record][field]) == str(data_from_pg[record][field])
                else:
                    date_sqlite = parser.parse(data_sq[record][field]).strftime('%Y-%m-%d %H:%M:%S.%f')
                    date_pg = data_from_pg[record][field].strftime('%Y-%m-%d %H:%M:%S.%f')
                    assert date_sqlite == date_pg
