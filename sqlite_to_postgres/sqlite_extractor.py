
class SQLiteExtractor:

    def __init__(self, conn):
        self.conn = conn

    def get_cursor_from_sqlite(self, table):
        curs = self.conn.cursor()
        curs.execute(f'SELECT * FROM {table};')
        return curs

    def get_batch_from_sqlite(self, curs):
        """Получение части записей из SQLite."""
        return curs.fetchmany(5000)
