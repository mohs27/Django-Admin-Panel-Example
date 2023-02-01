from psycopg2.extensions import connection
from psycopg2.extras import execute_batch


class PostgresSaver:
    def __init__(self, pg_conn: connection):
        self.pg_conn = pg_conn
        self.cursor = self.pg_conn.cursor()

    def truncate_table(self, table: str) -> None:
        """Remove table's content."""
        self.cursor.execute(f"""TRUNCATE content.{table} CASCADE;""")

    def save_data_to_postgres(
        self,
        data_vals: tuple,
        data_keys: tuple,
        table_name: str,
    ) -> None:
        """Основной метод сохранения данных в Postgres."""
        query = """INSERT INTO content.{0} ({1}) VALUES ({2}) ON CONFLICT (id) DO NOTHING""".format(
            table_name,
            ", ".join(map(str, data_keys)),
            ", ".join(["%s"] * len(data_keys)),
        )
        execute_batch(cur=self.cursor, sql=query, argslist=data_vals, page_size=100)

    def get_cursor_from_postgres(self, table):
        """Получение курсора из Postgres."""
        self.cursor.execute(f"SELECT * FROM {table};")
        return self.cursor

    def get_batch_from_postgres(self) -> tuple:
        """Получение части записей из Postgres."""
        return self.cursor.fetchmany(5000)
