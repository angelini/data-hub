import psycopg2 as psql
import psycopg2.extras

from core.data import Hub, Dataset, Backend, Backends, DatasetVersion, \
    PublishedVersion, Type, Types, Column, Partition, write

psql.extras.register_uuid()


def truncate(cursor, kind):
    cursor.execute(f'TRUNCATE TABLE {kind.table_name} CASCADE')


if __name__ == '__main__':
    conn = psql.connect('dbname=dh user=postgres host=localhost')
    cursor = conn.cursor()

    for kind in [Hub, Dataset, Backend, DatasetVersion, PublishedVersion, Type, Column, Partition]:
        truncate(cursor, kind)

    for entry in Types:
        write(cursor, entry)

    for entry in Backends:
        write(cursor, entry)

    conn.commit()
