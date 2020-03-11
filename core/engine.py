import abc
import dataclasses as dc
import datetime as dt
import pathlib as pl
import uuid
import typing as t

import psycopg2 as psql
import pytz

from core.data import Backend, Column, Dataset, DatasetVersion, Hub, Partition, Type, write


class Action(abc.ABC):

    @abc.abstractmethod
    def execute(self, cursor):
        pass


@dc.dataclass
class NewHub(Action):
    name:      str
    hive_host: str

    def execute(self, cursor):
        hub_id = uuid.uuid4()
        write(cursor, Hub(hub_id, self.name, self.hive_host, dt.datetime.now(tz=pytz.utc)))
        return hub_id


@dc.dataclass
class NewDataset(Action):
    hub_id: uuid.UUID
    name:   str

    def execute(self, cursor):
        dataset_id = uuid.uuid4()
        write(cursor, Dataset(self.hub_id, dataset_id, self.name, dt.datetime.now(tz=pytz.utc), None))
        return dataset_id


@dc.dataclass
class NewDatasetVersion(Action):
    hub_id:         uuid.UUID
    dataset_id:     uuid.UUID
    backend:        str
    path:           pl.Path
    description:    str
    is_overlapping: bool
    columns:        t.List[t.Tuple[str, str, str, bool, bool, bool]]

    def execute(self, cursor):
        cursor.execute('''
            SELECT max(version)
            FROM dataset_versions
            WHERE
                hub_id = %s
            AND dataset_id = %s
        ''', (self.hub_id, self.dataset_id))
        latest_version = cursor.fetchone()[0] or 0

        write(cursor, DatasetVersion(self.hub_id,
                                     self.dataset_id,
                                     latest_version + 1,
                                     Backend.by_module(self.backend).id,
                                     self.path,
                                     self.description,
                                     self.is_overlapping,
                                     dt.datetime.now(tz=pytz.utc)))

        position = 0
        for column in self.columns:
            name, type_name, description, is_nullable, is_unique, has_pii = column
            write(cursor, Column(self.hub_id,
                                 self.dataset_id,
                                 latest_version + 1,
                                 name,
                                 Type.by_name(type_name).id,
                                 position,
                                 description,
                                 is_nullable,
                                 is_unique,
                                 has_pii))
            position += 1

        return latest_version + 1


@dc.dataclass
class NewPartition(Action):
    hub_id:     uuid.UUID
    dataset_id: uuid.UUID
    version:    int

    values:     t.List[str]
    path:       str
    row_count:  t.Optional[int]
    start_time: t.Optional[dt.datetime]
    end_time:   t.Optional[dt.datetime]

    def execute(self, cursor):
        partition_id = uuid.uuid4()
        write(cursor, Partition(self.hub_id,
                                self.dataset_id,
                                self.version,
                                partition_id,
                                self.values,
                                self.path,
                                self.row_count,
                                self.start_time,
                                self.end_time,
                                dt.datetime.now(tz=pytz.utc),
                                None))
        return partition_id


class View(abc.ABC):

    @abc.abstractmethod
    def fetch(self, cursor):
        pass


@dc.dataclass
class ListHubs(View):

    def fetch(self, cursor):
        cursor.execute('''
            SELECT id, name, hive_host, created_at
            FROM hubs
            ORDER BY created_at
        ''')
        return {
            'hubs': [
                {
                    'id': row[0],
                    'name': row[1],
                    'hive_host': row[2],
                    'created_at': row[3],
                }
                for row in cursor.fetchall()
            ]
        }


@dc.dataclass
class ListDatasets(View):
    hub_id: uuid.UUID

    def fetch(self, cursor):
        cursor.execute('''
            SELECT id, name, version, created_at, published_at
            FROM datasets_with_current_versions
            WHERE hub_id = %s
            ORDER BY created_at
        ''', (self.hub_id,))
        return {
            'datasets': [
                {
                    'hub_id': self.hub_id,
                    'id': row[0],
                    'name': row[1],
                    'version': row[2],
                    'created_at': row[3],
                    'published_at': row[4],
                }
                for row in cursor.fetchall()
            ]
        }


@dc.dataclass
class ListVersions(View):
    hub_id:     uuid.UUID
    dataset_id: uuid.UUID

    def fetch(self, cursor):
        cursor.execute('''
            SELECT version, module, path, description, created_at
            FROM versions_with_backend
            WHERE
                hub_id = %s
            AND dataset_id = %s
            ORDER BY created_at
        ''', (self.hub_id, self.dataset_id))
        return {
            'versions': [
                {
                    'hub_id': self.hub_id,
                    'dataset_id': self.dataset_id,
                    'version': row[0],
                    'module': row[1],
                    'path': row[2],
                    'description': row[3],
                    'created_at': row[4],
                }
                for row in cursor.fetchall()
            ]
        }


@dc.dataclass
class DetailVersion(View):
    hub_id:     uuid.UUID
    dataset_id: uuid.UUID
    version:    int

    def fetch(self, cursor):
        cursor.execute('''
            SELECT name, type_name, description, is_nullable, is_unique, has_pii
            FROM columns_with_type
            WHERE
                hub_id = %s
            AND dataset_id = %s
            AND version = %s
            ORDER BY position
        ''', (self.hub_id, self.dataset_id, self.version))
        columns = [{
            'name': row[0],
            'type_name': row[1],
            'description': row[2],
            'is_nullable': row[3],
            'is_unique': row[4],
            'has_pii': row[5],
        } for row in cursor.fetchall()]

        cursor.execute('''
            SELECT vals, path, row_count, start_time, end_time, created_at
            FROM partitions
            WHERE
                hub_id = %s
            AND dataset_id = %s
            AND version = %s
            ORDER BY end_time DESC;
        ''', (self.hub_id, self.dataset_id, self.version))
        partitions = [{
            'vals': row[0],
            'path': row[1],
            'row_count': row[2],
            'start_time': row[3],
            'end_time': row[4],
            'created_at': row[5],
        } for row in cursor.fetchall()]

        cursor.execute('''
            SELECT keys, module, path, description, created_at
            FROM versions_with_backend
            WHERE
                hub_id = %s
            AND dataset_id = %s
            AND version = %s
        ''', (self.hub_id, self.dataset_id, self.version))
        row = cursor.fetchone()

        return {
            'version': {
                'hub_id': self.hub_id,
                'dataset_id': self.dataset_id,
                'version': self.version,
                'keys': row[0],
                'module': row[1],
                'path': row[2],
                'description': row[3],
                'created_at': row[4],
            },
            'columns': columns,
            'partitions': partitions,
        }


def execute(action):
    conn = psql.connect('dbname=dh user=postgres')
    action.execute(conn.cursor())
    conn.commit()
