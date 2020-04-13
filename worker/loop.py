import importlib
import time

import pq
import psycopg2 as psql
import psycopg2.extras

from core.engine import ListBackends
from core.job import Job

psql.extras.register_uuid()


def load_backends(cursor):
    return {
        backend['id']: importlib.import_module(backend['module'])
        for backend in ListBackends().fetch(cursor)
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
    try:
        main()
    except KeyboardInterrupt:
        print('\nstopping worker')
