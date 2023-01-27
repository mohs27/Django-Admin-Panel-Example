import os
import sqlite3
from contextlib import contextmanager


class SQLiteExtractor:

    def __init__(self, conn):
        self.conn = conn

    @contextmanager
    def conn_context(self, db_path: str):
        self.conn = sqlite3.connect(db_path)
        self.conn.row_factory = sqlite3.Row
        yield self.conn
        self.conn.close()

    def extract_data(self):
        db_path = os.environ.get('SQLITE_PATH')
        with self.conn_context(db_path) as conn:
            curs = conn.cursor()
            curs.execute('SELECT * FROM film_work;')
            data_film_work = curs.fetchall()
            curs.execute('SELECT * FROM genre;')
            data_genre = curs.fetchall()
            curs.execute('SELECT * FROM person;')
            data_person = curs.fetchall()

            curs.execute('SELECT * FROM genre_film_work;')
            data_genre_film_work = curs.fetchall()

            curs.execute('SELECT * FROM person_film_work;')
            data_person_film_work = curs.fetchall()

            return data_film_work, data_person, data_genre, data_genre_film_work, data_person_film_work
