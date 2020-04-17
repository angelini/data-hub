import importlib
import time

import pq
import psycopg2 as psql
import psycopg2.extras

from core.engine import logging
from core.engine.views import ListBackends
from core.job import Job

psql.extras.register_uuid()


def load_backends(cursor):
    return {
        backend['id']: importlib.import_module(backend['module'])
        for backend in ListBackends().fetch(cursor)['backends']
    }


def run_job(cursor, backend, job):
    start_time = time.time()
    logging.info('start_job',
                 action=job.action,
                 backend=backend.__name__,
                 **job.config)

    fn = backend.__getattribute__(job.action)
    fn(cursor, **job.config)

    logging.info('end_job',
                 action=job.action,
                 backend=backend.__name__,
                 time=round((time.time() - start_time) * 1000, ndigits=4),
                 **job.config)


def main():
    logging.configure()

    conn = psql.connect('')
    queue = pq.PQ(conn=conn)['jobs']

    backends = load_backends(conn.cursor())

    for job_entry in queue:
        if job_entry is None:
            time.sleep(2)
            continue

        job = Job(**job_entry.data)
        backend = backends[job.backend_id]

        run_job(conn.cursor(), backend, job)
        conn.commit()


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('\nstopping worker')
