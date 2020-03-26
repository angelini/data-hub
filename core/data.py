import abc
import dataclasses as dc
import datetime as dt
import pathlib as pl
import random
import typing as t
import uuid

import faker
import pytz


class Entry(abc.ABC):
    table_name: str

    @abc.abstractproperty
    def table_name():
        pass

    @abc.abstractstaticmethod
    def sample():
        pass

    @property
    def columns(self):
        return [field.name for field in dc.fields(self)]

    @property
    def values(self):
        return [self.__getattribute__(column) for column in self.columns]


@dc.dataclass
class Hub(Entry):
    id:         uuid.UUID

    name:       str
    hive_host:  pl.Path
    created_at: dt.datetime

    table_name = 'hubs'

    @staticmethod
    def sample():
        fake = faker.Faker()
        return Hub(uuid.uuid4(),
                   fake.simple_profile()['username'],
                   fake.unix_device(),
                   dt.datetime.now(tz=pytz.utc))


@dc.dataclass
class Dataset(Entry):
    hub_id: uuid.UUID
    id:     uuid.UUID

    name: str
    created_at: dt.datetime
    deleted_at: t.Optional[dt.datetime]

    table_name = 'datasets'

    @staticmethod
    def sample(hub_id):
        fake = faker.Faker()
        return Dataset(hub_id, uuid.uuid4(), fake.name(), dt.datetime.now(tz=pytz.utc), None)


@dc.dataclass
class Backend(Entry):
    id: int

    module: str

    table_name = 'backends'

    @staticmethod
    def sample():
        return random.choice(Backends)

    @staticmethod
    def by_module(module):
        for backend in Backends:
            if backend.module == module:
                return backend
        raise ValueError(f'Unknown backend: {module}')


FileBackend = Backend(1, "backends.fs")
S3Backend = Backend(2, "backends.s3")

Backends = [
    FileBackend,
    S3Backend,
]


@dc.dataclass
class DatasetVersion(Entry):
    hub_id:     uuid.UUID
    dataset_id: uuid.UUID
    version:    int

    backend_id:     int
    path:           pl.Path
    partition_keys: t.List[str]
    description:    t.Optional[str]
    is_overlapping: str
    created_at:     dt.datetime

    table_name = 'dataset_versions'

    @staticmethod
    def sample(hub_id, dataset_id, count=0, keys=None):
        fake = faker.Faker()
        return DatasetVersion(hub_id,
                              dataset_id,
                              count,
                              Backend.sample().id,
                              fake.file_path(),
                              keys or [],
                              "",
                              True,
                              dt.datetime.now(tz=pytz.utc))


@dc.dataclass
class Dependency(Entry):
    parent_hub_id:     uuid.UUID
    parent_dataset_id: uuid.UUID
    parent_version:    int
    child_hub_id:      uuid.UUID
    child_dataset_id:  uuid.UUID
    child_version:     int

    table_name = 'dependencies'

    @staticmethod
    def sample(parent_hub_id, parent_dataset_id, parent_version,
               child_hub_id, child_dataset_id, child_version):
        return Dependency(parent_hub_id,
                          parent_dataset_id,
                          parent_version,
                          child_hub_id,
                          child_dataset_id,
                          child_version)


@dc.dataclass
class PublishedVersion(Entry):
    hub_id:     uuid.UUID
    dataset_id: uuid.UUID
    version:    int

    published_at: dt.datetime

    table_name = 'published_versions'

    @staticmethod
    def sample(hub_id, dataset_id, version):
        return PublishedVersion(hub_id, dataset_id, version, dt.datetime.now(tz=pytz.utc))


@dc.dataclass
class Type(Entry):
    id: int

    name:      str
    hive_type: str

    table_name = 'types'

    @staticmethod
    def sample():
        return random.choice(Types)

    @staticmethod
    def by_name(name):
        for type_entry in Types:
            if type_entry.name == name:
                return type_entry
        raise ValueError(f'Unknown type: {name}')


IntType = Type(1, 'int', 'int')
DoubleType = Type(2, 'double', 'double')
StringType = Type(3, 'string', 'string')

Types = [
    IntType,
    DoubleType,
    StringType,
]


@dc.dataclass
class Column(Entry):
    hub_id:     uuid.UUID
    dataset_id: uuid.UUID
    version:    int
    name:       str

    type_id:     int
    position:    int
    description: t.Optional[str]
    is_nullable: bool
    is_unique:   bool
    has_pii:     bool

    table_name = 'columns'

    @staticmethod
    def sample(hub_id, dataset_id, version, position=0):
        fake = faker.Faker()
        return Column(hub_id,
                      dataset_id,
                      version,
                      fake.simple_profile()['username'],
                      Type.sample().id,
                      position,
                      fake.sentence(nb_words=6, variable_nb_words=True),
                      True,
                      False,
                      False)


@dc.dataclass
class Partition(Entry):
    hub_id:     uuid.UUID
    dataset_id: uuid.UUID
    version:    int
    id:         uuid.UUID

    path:             pl.Path
    partition_values: t.List[str]
    row_count:        t.Optional[int]
    start_time:       t.Optional[dt.datetime]
    end_time:         t.Optional[dt.datetime]
    created_at:       dt.datetime
    deleted_at:       t.Optional[dt.datetime]

    table_name = 'partitions'

    @staticmethod
    def sample(hub_id, dataset_id, version, values):
        end = dt.datetime.now(tz=pytz.UTC).replace(hour=0, minute=0, second=0, microsecond=0)
        start = end - dt.timedelta(days=random.randint(1, 4))

        fake = faker.Faker()
        return Partition(hub_id,
                         dataset_id,
                         version,
                         uuid.uuid4(),
                         values,
                         fake.file_path(),
                         random.randint(100, 2000),
                         start,
                         end,
                         dt.datetime.now(tz=pytz.UTC),
                         None)


def write(cursor, entry):
    cursor.execute(f'INSERT INTO {entry.table_name} '
                   f'({", ".join(entry.columns)}) '
                   f'VALUES ({", ".join(["%s" for _ in entry.columns])})',
                   entry.values)
