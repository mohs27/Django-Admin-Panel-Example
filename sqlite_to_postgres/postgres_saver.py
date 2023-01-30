import psycopg2

from sqlite_to_postgres.data_structure import (FilmWork, Genre, GenreFilmWork,
                                               Person, PersonFilmWork)


class PostgresSaver:
    def __init__(self,  pg_conn: psycopg2.extensions.connection):
        self.pg_conn = pg_conn

    def truncate_table(self, table):
        cursor = self.pg_conn.cursor()
        cursor.execute(f"""TRUNCATE content.{table} CASCADE;""")

    def save_film_work_to_postgres(self, film_work: FilmWork):
        cursor = self.pg_conn.cursor()

        args = ','.join(cursor.mogrify('(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)', item).decode() for item in film_work)
        cursor.execute(
            f"""INSERT INTO content.film_work (
            created, modified, id, title, description, creation_date, rating, type, certificate, file_path
            )
            VALUES {args}
            """)

    def save_genre_to_postgres(self, genre: Genre):
        cursor = self.pg_conn.cursor()

        args = ','.join(cursor.mogrify('(%s, %s, %s, %s, %s)', item).decode() for item in genre)
        cursor.execute(f"""
            INSERT INTO content.genre (created, modified, id, name, description)
            VALUES {args}
            """)

    def save_person_to_postgres(self, person: Person):
        cursor = self.pg_conn.cursor()

        args = ','.join(cursor.mogrify('(%s, %s, %s, %s, %s)', item).decode() for item in person)
        cursor.execute(f"""
               INSERT INTO content.person (created, modified, id, full_name, gender)
               VALUES {args}
               """)

    def save_genre_film_work_to_postgres(self, genre_film_work: GenreFilmWork):
        cursor = self.pg_conn.cursor()

        args = ','.join(cursor.mogrify('(%s, %s, %s, %s)', item).decode() for item in genre_film_work)
        cursor.execute(f"""
               INSERT INTO content.genre_film_work (id, created, film_work_id, genre_id)
               VALUES {args}
               """)

    def save_person_film_work_to_postgres(self, person_film_work: PersonFilmWork):
        cursor = self.pg_conn.cursor()

        args = ','.join(cursor.mogrify('(%s, %s, %s, %s, %s)', item).decode() for item in person_film_work)
        cursor.execute(f"""
               INSERT INTO content.person_film_work (id, role, created, film_work_id, person_id)
               VALUES {args}
               """)

    def get_cursor_from_postgres(self, table):
        """Получение курсора из Postgres."""
        cursor = self.pg_conn.cursor()
        cursor.execute(f'SELECT * FROM {table};')
        return cursor

    def get_batch_from_postgres(self, curs):
        """Получение части записей из Postgres."""
        return curs.fetchmany(5000)
