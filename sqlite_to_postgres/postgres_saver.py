from dataclasses import asdict

import psycopg2


class PostgresSaver:
    def __init__(self,  pg_conn: psycopg2.extensions.connection):
        self.pg_conn = pg_conn

    def truncate_table(self, table):
        cursor = self.pg_conn.cursor()
        cursor.execute(f"""TRUNCATE content.{table} CASCADE;""")

    def save_data_to_postgres(self, movie, table_name):
        cursor = self.pg_conn.cursor()
        dictionary = asdict(movie, dict_factory=lambda dic: {key: value for (key, value) in dic if value is not None})
        query = """INSERT INTO content.{} ({}) VALUES ({}) """.format(
            table_name,
            ', '.join(map(str, (dictionary.keys()))),
            ', '.join(['%s'] * len(dictionary)),
        )
        cursor.execute(query, tuple(dictionary.values()))

    def get_cursor_from_postgres(self, table):
        """Получение курсора из Postgres."""
        cursor = self.pg_conn.cursor()
        cursor.execute(f'SELECT * FROM {table};')
        return cursor

    def get_batch_from_postgres(self, curs):
        """Получение части записей из Postgres."""
        return curs.fetchmany(5000)
