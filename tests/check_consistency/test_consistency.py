import datetime
import os
import sqlite3
from contextlib import closing

import psycopg2
from dateutil import parser
from dotenv import load_dotenv
from psycopg2.extras import DictCursor

from sqlite_to_postgres.postgres_saver import PostgresSaver
from sqlite_to_postgres.sqlite_extractor import SQLiteExtractor

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

PARENT_DIR = os.path.split(os.path.abspath(BASE_DIR))[0]

dotenv_path = os.path.join(PARENT_DIR, "tests.env")

load_dotenv(dotenv_path)


def connect_to_databases(table_sqlite: str, table_pg: str) -> tuple[list, list]:
    """Соединение с постргресом и скулайт для получения всех данных."""
    all_data_sq = []
    all_data_from_pg = []
    dsl = {
        "dbname": os.environ.get("DB_NAME"),
        "user": os.environ.get("DB_USER"),
        "password": os.environ.get("DB_PASSWORD"),
        "host": os.environ.get("DB_HOST"),
        "port": os.environ.get("DB_PORT"),
    }
    with sqlite3.connect(os.environ.get("SQLITE_PATH")) as sqlite_conn, closing(
        psycopg2.connect(**dsl, cursor_factory=DictCursor),
    ) as pg_conn:
        sqlite_extractor = SQLiteExtractor(sqlite_conn)
        postgres_saver = PostgresSaver(pg_conn)

        sqlite_extractor.get_cursor_from_sqlite(table_sqlite)
        postgres_saver.get_cursor_from_postgres(table_pg)
        while True:
            data_sq = sqlite_extractor.get_batch_from_sqlite()
            all_data_sq.extend(data_sq)

            data_from_pg = postgres_saver.get_batch_from_postgres()
            all_data_from_pg.extend(data_from_pg)
            if not data_sq or not data_from_pg:
                break

        return all_data_sq, all_data_from_pg


def test_records_count() -> None:
    """Тестирование соответствия количества записей при переносе данных из SQLite в PostgreSQL."""
    tables_sqlite = [
        "genre",
        "person",
        "film_work",
        "genre_film_work",
        "person_film_work",
    ]
    for table in tables_sqlite:
        data_sq, data_from_pg = connect_to_databases(
            table_sqlite=table,
            table_pg="content." + table,
        )
        assert len(data_sq) == len(data_from_pg)


def test_consistency() -> None:
    """Тестирование соответствия содержимого записей при переносе данных из SQLite в PostgreSQL."""
    tables_sqlite = [
        "genre",
        "person",
        "film_work",
        "genre_film_work",
        "person_film_work",
    ]
    for table in tables_sqlite:
        data_sq, data_from_pg = connect_to_databases(
            table_sqlite=table,
            table_pg="content." + table,
        )
        for record in range(0, len(data_sq)):
            for field in range(0, len(data_sq[record])):
                if not isinstance(data_from_pg[record][field], datetime.date):
                    assert str(data_sq[record][field]) == str(
                        data_from_pg[record][field],
                    )
                else:
                    date_sqlite = parser.parse(data_sq[record][field]).strftime(
                        "%Y-%m-%d %H:%M:%S.%f",
                    )
                    date_pg = data_from_pg[record][field].strftime(
                        "%Y-%m-%d %H:%M:%S.%f",
                    )
                    assert date_sqlite == date_pg
