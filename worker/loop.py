import importlib
import time

import pq
import psycopg2 as psql
import psycopg2.extras

from core.job import Job

psql.extras.register_uuid()


def load_backends(cursor):
    cursor.execute('''
        SELECT id, module
        FROM backends
    ''')
    return {
        row[0]: importlib.import_module(row[1])
        for row in cursor.fetchall()
    }


def main():
    conn = psql.connect('')
    queue = pq.PQ(conn=conn)['jobs']

    backends = load_backends(conn.cursor())

    for job_entry in queue:
        if job_entry is None:
            time.sleep(2)
            continue

        job = Job(**job_entry.data)
        backend = backends[job.backend_id]

        cursor = conn.cursor()
        fn = backend.__getattribute__(job.action)

        fn(cursor, **job.config)
        conn.commit()


if __name__ == '__main__':
    main()
