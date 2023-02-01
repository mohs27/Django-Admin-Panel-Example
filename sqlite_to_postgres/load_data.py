import logging
import os
import sqlite3
import typing
from contextlib import closing, contextmanager
from dataclasses import asdict, fields

import psycopg2
from dotenv import load_dotenv
from psycopg2.extensions import connection as _connection
from psycopg2.extras import DictCursor

from sqlite_to_postgres.data_structure import (
    FilmWork,
    Genre,
    GenreFilmWork,
    Person,
    PersonFilmWork,
)
from sqlite_to_postgres.postgres_saver import PostgresSaver
from sqlite_to_postgres.sqlite_extractor import SQLiteExtractor

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

PARENT_DIR = os.path.split(os.path.abspath(BASE_DIR))[0]

dotenv_path = os.path.join(PARENT_DIR + "\\config\\", ".env")

load_dotenv(dotenv_path)


def class_from_args(class_name: typing.ClassVar, arg_dict: dict):
    """Class from Args."""
    field_set = {field.name for field in fields(class_name) if field.init}
    filtered_arg_dict = {
        key: value for key, value in arg_dict.items() if key in field_set
    }
    return class_name(**filtered_arg_dict)


def load_from_sqlite(connection: sqlite3.Connection, pg_conn: _connection) -> None:
    """Основной метод загрузки данных из SQLite в Postgres."""
    table_class = {
        "film_work": FilmWork,
        "genre": Genre,
        "person": Person,
        "genre_film_work": GenreFilmWork,
        "person_film_work": PersonFilmWork,
    }
    sqlite_extractor = SQLiteExtractor(connection)
    postgres_saver = PostgresSaver(pg_conn)

    for table in table_class.keys():
        prepared_data = []
        logging.info(f"Table: {table}")
        postgres_saver.truncate_table(table)

        sqlite_extractor.get_data_and_cursor_from_sqlite(table)

        while True:
            data = sqlite_extractor.get_batch_from_sqlite()
            if data:
                for row in data:
                    data_cls = class_from_args(
                        table_class[f"{table}"],
                        dict(row),
                    )
                    dictionary = asdict(
                        data_cls,
                        dict_factory=lambda dic: {key: value for (key, value) in dic},
                    )
                    data_keys = tuple(dictionary.keys())
                    prepared_data.append(tuple(dictionary.values()))

                postgres_saver.save_data_to_postgres(
                    tuple(prepared_data),
                    data_keys,
                    f"{table}",
                )
            else:
                break


@contextmanager
def open_db(file_name: str):
    """Открытие соединения с базой данных SQLite."""
    conn = sqlite3.connect(file_name)
    conn.row_factory = sqlite3.Row
    try:
        logging.info("Creating connection")
        yield conn
    finally:
        logging.info("Closing connection")
        conn.commit()
        conn.close()


if __name__ == "__main__":
    dsl = {
        "dbname": os.environ.get("DB_NAME"),
        "user": os.environ.get("DB_USER"),
        "password": os.environ.get("DB_PASSWORD"),
        "host": os.environ.get("DB_HOST"),
        "port": os.environ.get("DB_PORT"),
    }
    logging.basicConfig(level=logging.INFO)

    with open_db(file_name=os.environ.get("SQLITE_PATH")) as sqlite_conn, closing(
        psycopg2.connect(**dsl, cursor_factory=DictCursor),
    ) as pg_conn:
        try:
            load_from_sqlite(sqlite_conn, pg_conn)
        except Exception as exc:
            logging.info(exc)
