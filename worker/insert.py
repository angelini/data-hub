import argparse

import pq
import psycopg2 as psql
import psycopg2.extras

from core.job import Job

psql.extras.register_uuid()


def main():
    conn = psql.connect('')
    queue = pq.PQ(conn=conn)['jobs']

    queue.put(Job(1, 'test', {}).__dict__)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    args = parser.parse_args()

    main()
