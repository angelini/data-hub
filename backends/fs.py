import pathlib

from core.data import Status
from core.engine import logging
from core.engine.actions import NewPartition, UpdatePartitionStatus
from core.engine.assertions import PartitionPathExists
from core.engine.views import ListPartitions, SimpleDetailVersion


def __directory_exists(path):
    if isinstance(path, str):
        path = pathlib.Path(path)
    return path.exists() and path.is_dir()


def verify_partitions(cursor, hub_id, dataset_id, version):
    partitions = ListPartitions(hub_id, dataset_id, version).fetch(cursor)['partitions']
    for partition in partitions:
        status = Status.OK if __directory_exists(partition['path']) else Status.ERROR
        logging.info('partition_status', partition_id=partition['id'], status=status, path=partition['path'])
        UpdatePartitionStatus(partition['id'], status).execute(cursor)


def __find_nested_partitions(directory, keys, values):
    if not keys:
        yield {
            'path': directory,
            'values': values,
        }

    for child_dir in directory.iterdir():
        dir_kv = child_dir.name.split('=')
        if dir_kv[0] != keys[0]:
            raise ValueError(f'Key "{keys[0]}" does not match on disk folder "{dir_kv[0]}"')

        for partition in __find_nested_partitions(child_dir, keys[1:], values + [dir_kv[1]]):
            yield partition


def discover_partitions(cursor, hub_id, dataset_id, version):
    details = SimpleDetailVersion(hub_id, dataset_id, version).fetch(cursor)
    path = pathlib.Path(details['path'])
    if not __directory_exists(path):
        raise ValueError(f'Path {details["path"]} does not exist')

    for partition in __find_nested_partitions(path, details['partition_keys'], []):
        exists = PartitionPathExists(hub_id, dataset_id, version, str(partition['path']), partition['values'])
        if exists.check(cursor):
            logging.warn('skip_existing_partition',
                         hub_id=hub_id,
                         dataset_id=dataset_id,
                         version=version,
                         path=partition['path'],
                         values=partition['values'])
        else:
            NewPartition(hub_id, dataset_id, version,
                         str(partition['path']), partition['values'],
                         None, None, None).execute(cursor)
