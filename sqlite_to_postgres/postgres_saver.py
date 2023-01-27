import psycopg2
from data_structure import (
    FilmWork, Genre, GenreFilmWork, Person, PersonFilmWork,
)


class PostgresSaver:
    def __init__(self,  pg_conn: psycopg2.extensions.connection):
        self.pg_conn = pg_conn

    def save_film_work_to_postgres(self, film_work: FilmWork):
        cursor = self.pg_conn.cursor()
        cursor.execute("""TRUNCATE content.film_work CASCADE;""")

        args = ','.join(cursor.mogrify('(%s, %s, %s, %s, %s, %s, %s, %s, %s)', item).decode() for item in film_work)
        cursor.execute(f"""
            INSERT INTO content.film_work (
            id, title, description, creation_date, file_path, rating, type, created, modified
            )
            VALUES {args}
            """)

    def save_genre_to_postgres(self, genre: Genre):
        cursor = self.pg_conn.cursor()
        cursor.execute("""TRUNCATE content.genre CASCADE;""")

        args = ','.join(cursor.mogrify('(%s, %s, %s, %s, %s)', item).decode() for item in genre)
        cursor.execute(f"""
            INSERT INTO content.genre (id, name, description, created, modified)
            VALUES {args}
            """)

    def save_person_to_postgres(self, person: Person):
        cursor = self.pg_conn.cursor()
        cursor.execute("""TRUNCATE content.person CASCADE;""")

        args = ','.join(cursor.mogrify('(%s, %s, %s, %s)', item).decode() for item in person)
        cursor.execute(f"""
               INSERT INTO content.person (id, full_name, created, modified)
               VALUES {args}
               """)

    def save_genre_film_work_to_postgres(self, genre_film_work: GenreFilmWork):
        cursor = self.pg_conn.cursor()
        cursor.execute("""TRUNCATE content.genre_film_work CASCADE;""")

        args = ','.join(cursor.mogrify('(%s, %s, %s, %s)', item).decode() for item in genre_film_work)
        cursor.execute(f"""
               INSERT INTO content.genre_film_work (id, film_work_id, genre_id, created)
               VALUES {args}
               """)

    def save_person_film_work_to_postgres(self, person_film_work: PersonFilmWork):
        cursor = self.pg_conn.cursor()
        cursor.execute("""TRUNCATE content.person_film_work CASCADE;""")

        args = ','.join(cursor.mogrify('(%s, %s, %s, %s, %s)', item).decode() for item in person_film_work)
        cursor.execute(f"""
               INSERT INTO content.person_film_work (id, film_work_id, person_id, role, created)
               VALUES {args}
               """)
