import argparse

import pq
import psycopg2 as psql
import psycopg2.extras

from core.job import Job

psql.extras.register_uuid()


def main(hub_id, dataset_id, version):
    conn = psql.connect('')
    queue = pq.PQ(conn=conn)['jobs']

    queue.put(
        Job(1, 'verify_partitions', {
            'hub_id': hub_id,
            'dataset_id': dataset_id,
            'version': version,
        }).__dict__
    )


if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    parser.add_argument('hub_id')
    parser.add_argument('dataset_id')
    parser.add_argument('version')

    args = parser.parse_args()

    main(args.hub_id, args.dataset_id, args.version)
