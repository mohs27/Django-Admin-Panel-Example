import contextlib
import os
import random
import uuid
from datetime import datetime

import psycopg2
from dotenv import load_dotenv
from faker import Faker
from psycopg2.extras import execute_batch

fake = Faker()

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

PARENT_DIR = os.path.split(os.path.abspath(BASE_DIR))[0]

dotenv_path = os.path.join(PARENT_DIR+'\\config\\', '.env')

load_dotenv(dotenv_path)

dsn = {
        'dbname': os.environ.get("DB_NAME"), 'user': os.environ.get("DB_USER"),
        'password': os.environ.get("DB_PASSWORD"), 'host': os.environ.get("DB_HOST"),
        'port':  os.environ.get("DB_PORT"), 'options': '-c search_path=content',
    }

PERSONS_COUNT = 100000
PAGE_SIZE = 5000

now = datetime.utcnow()

with contextlib.closing(psycopg2.connect(**dsn)) as conn, conn.cursor() as cur:

    persons_ids = [str(uuid.uuid4()) for _ in range(PERSONS_COUNT)]
    query = 'INSERT INTO person (id, full_name, created, modified) VALUES (%s, %s, %s, %s)'
    data = [(pk, fake.last_name(), now, now) for pk in persons_ids]
    execute_batch(cur, query, data, page_size=PAGE_SIZE)
    conn.commit()

    person_film_work_data = []
    roles = ['actor', 'producer', 'director']

    cur.execute('SELECT id FROM film_work')
    while True:
        film_works_ids = [data[0] for data in cur.fetchmany(5000)]
        for film_work_id in film_works_ids:
            for person_id in random.sample(persons_ids, 5):
                role = random.choice(roles)
                person_film_work_data.append(
                    (str(uuid.uuid4()), film_work_id, person_id, role, now),
                )

        query = 'INSERT INTO person_film_work (id, film_work_id, person_id, role, created) ' \
                'VALUES (%s, %s, %s, %s, %s) ON CONFLICT (id, film_work_id, person_id) DO NOTHING; '
        execute_batch(
            conn.cursor(), query, person_film_work_data, page_size=PAGE_SIZE,
        )
        conn.commit()
        if not film_works_ids:
            break
