import abc
import dataclasses as dc
import datetime as dt
import pathlib as pl
import typing as t
import uuid


class AccessLevel:
    ADMIN = 'admin'
    WRITER = 'writer'
    READER = 'reader'
    NONE = 'none'

    _order = [ADMIN, WRITER, READER, NONE]

    @classmethod
    def highest_level(cls, *args):
        for level in cls._order:
            if level in args:
                return level
        return 'none'

    @classmethod
    def can_read(cls, level):
        return cls._order.index(level) <= cls._order.index('reader')

    @classmethod
    def can_write(cls, level):
        return cls._order.index(level) <= cls._order.index('writer')


class Entry(abc.ABC):
    table_name: str

    @property
    def columns(self):
        return [field.name for field in dc.fields(self)]

    @property
    def values(self):
        return [self.__getattribute__(column) for column in self.columns]


@dc.dataclass
class User(Entry):
    id: uuid.UUID

    email:         str
    password_hash: str
    created_at:    dt.datetime

    table_name = 'users'


@dc.dataclass
class Team(Entry):
    id: uuid.UUID

    name:       str
    created_at: dt.datetime

    table_name = 'teams'


@dc.dataclass
class TeamMember(Entry):
    id: uuid.UUID

    team_id:    uuid.UUID
    user_id:    uuid.UUID
    created_at: dt.datetime
    deleted_at: t.Optional[dt.datetime]

    table_name = 'team_members'


@dc.dataclass
class Hub(Entry):
    id: uuid.UUID

    team_id:    uuid.UUID
    name:       str
    created_at: dt.datetime

    table_name = 'hubs'


@dc.dataclass
class TeamRole(Entry):
    id: uuid.UUID

    team_id:      uuid.UUID
    hub_id:       uuid.UUID
    access_level: str
    created_at:   dt.datetime

    table_name = 'team_roles'


@dc.dataclass
class Dataset(Entry):
    hub_id: uuid.UUID
    id:     uuid.UUID

    name:       str
    created_at: dt.datetime
    deleted_at: t.Optional[dt.datetime]

    table_name = 'datasets'


@dc.dataclass
class Backend(Entry):
    id: int

    module: str

    table_name = 'backends'

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


@dc.dataclass
class Dependency(Entry):
    parent_hub_id:     uuid.UUID
    parent_dataset_id: uuid.UUID
    parent_version:    int
    child_hub_id:      uuid.UUID
    child_dataset_id:  uuid.UUID
    child_version:     int

    table_name = 'dependencies'


@dc.dataclass
class PublishedVersion(Entry):
    hub_id:     uuid.UUID
    dataset_id: uuid.UUID
    version:    int

    published_at: dt.datetime

    table_name = 'published_versions'


@dc.dataclass
class Type(Entry):
    id: int

    name:      str

    table_name = 'types'

    @staticmethod
    def by_name(name):
        for type_entry in Types:
            if type_entry.name == name:
                return type_entry
        raise ValueError(f'Unknown type: {name}')


IntType = Type(1, 'int')
DoubleType = Type(2, 'double')
StringType = Type(3, 'string')

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


@dc.dataclass
class Partition(Entry):
    id:         uuid.UUID

    hub_id:           uuid.UUID
    dataset_id:       uuid.UUID
    version:          int
    path:             pl.Path
    partition_values: t.List[str]
    row_count:        t.Optional[int]
    start_time:       t.Optional[dt.datetime]
    end_time:         t.Optional[dt.datetime]
    created_at:       dt.datetime
    deleted_at:       t.Optional[dt.datetime]

    table_name = 'partitions'


def write(cursor, entry):
    cursor.execute(f'INSERT INTO {entry.table_name} '
                   f'({", ".join(entry.columns)}) '
                   f'VALUES ({", ".join(["%s" for _ in entry.columns])})',
                   entry.values)
