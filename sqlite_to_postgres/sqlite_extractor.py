
import sqlite3
from contextlib import contextmanager


class SQLiteExtractor:

    def __init__(self, conn):
        self.conn = conn

    @contextmanager
    def conn_context(self):
        self.conn.row_factory = sqlite3.Row
        yield self.conn
        self.conn.close()

    def get_data_from_sqlite(self, table):
        curs = self.conn.cursor()
        curs.execute(f'SELECT * FROM {table};')
        return curs.fetchall()
