import pathlib

from core.data import Status
from core.engine import ListPartitions, UpdatePartitionStatus


def verify_partition(path):
    return path.exists() and path.is_dir()


def verify_partitions(cursor, hub_id, dataset_id, version):
    partitions = ListPartitions(hub_id, dataset_id, version).fetch(cursor)['partitions']
    for partition in partitions:
        path = pathlib.Path(partition['path'])
        status = Status.OK if verify_partition(path) else Status.ERROR
        UpdatePartitionStatus(partition['id'], status).execute(cursor)
