import abc
import dataclasses as dc
import datetime as dt
import enum
import json
import pathlib as pl
import typing as t
import uuid


class AccessLevel(enum.Enum):
    ADMIN = 'admin'
    WRITER = 'writer'
    READER = 'reader'
    NONE = 'none'

    @classmethod
    def highest_level(cls, *args):
        for level in AccessLevelOrder:
            if level in args:
                return level
        return AccessLevel.NONE.value

    @classmethod
    def can_read(cls, level):
        return AccessLevelOrder.index(level) <= AccessLevelOrder.index('reader')

    @classmethod
    def can_write(cls, level):
        return AccessLevelOrder.index(level) <= AccessLevelOrder.index('writer')


AccessLevelOrder = [
    AccessLevel.ADMIN.value,
    AccessLevel.WRITER.value,
    AccessLevel.READER.value,
    AccessLevel.NONE.value,
]


class Status(enum.Enum):
    OK = 'ok'
    ERROR = 'error'
    QUEUED = 'queued'
    UNKNOWN = 'unknown'


class Entry(abc.ABC):
    table_name:   str
    primary_keys: t.Tuple[str]

    @property
    def columns(self):
        return [field.name for field in dc.fields(self)]

    @property
    def values(self):
        def get_value(column):
            value = self.__getattribute__(column)
            if isinstance(value, dict):
                return json.dumps(value)
            return value

        return [get_value(column) for column in self.columns]

    @property
    def secondary_columns(self):
        return [column
                for column in self.columns
                if column not in self.primary_keys]

    @property
    def secondary_values(self):
        indexes = [idx
                   for idx, column in enumerate(self.columns)
                   if column not in self.primary_keys]
        return [value
                for idx, value in enumerate(self.values)
                if idx in indexes]

@dc.dataclass
class User(Entry):
    id: uuid.UUID

    email:         str
    password_hash: str
    created_at:    dt.datetime

    table_name = 'users'
    primary_keys = ('id', )


@dc.dataclass
class Team(Entry):
    id: uuid.UUID

    name:       str
    created_at: dt.datetime

    table_name = 'teams'
    primary_keys = ('id', )


@dc.dataclass
class TeamMember(Entry):
    id: uuid.UUID

    team_id:    uuid.UUID
    user_id:    uuid.UUID
    created_at: dt.datetime
    deleted_at: t.Optional[dt.datetime]

    table_name = 'team_members'
    primary_keys = ('id', )


@dc.dataclass
class Hub(Entry):
    id: uuid.UUID

    team_id:    uuid.UUID
    name:       str
    created_at: dt.datetime

    table_name = 'hubs'
    primary_keys = ('id', )


@dc.dataclass
class TeamRole(Entry):
    id: uuid.UUID

    team_id:      uuid.UUID
    hub_id:       uuid.UUID
    access_level: str
    created_at:   dt.datetime

    table_name = 'team_roles'
    primary_keys = ('id', )


@dc.dataclass
class Dataset(Entry):
    hub_id: uuid.UUID
    id:     uuid.UUID

    name:       str
    created_at: dt.datetime
    deleted_at: t.Optional[dt.datetime]

    table_name = 'datasets'
    primary_keys = ('hub_id', 'id')


@dc.dataclass
class Backend(Entry):
    id: int

    module: str

    table_name = 'backends'
    primary_keys = ('id', )

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
    primary_keys = ('hub_id', 'dataset_id', 'version')


@dc.dataclass
class Dependency(Entry):
    parent_hub_id:     uuid.UUID
    parent_dataset_id: uuid.UUID
    parent_version:    int
    child_hub_id:      uuid.UUID
    child_dataset_id:  uuid.UUID
    child_version:     int

    table_name = 'dependencies'
    primary_keys = (
        'parent_hub_id', 'parent_dataset_id', 'parent_version',
        'child_hub_id', 'child_dataset_id', 'child_version',
    )


@dc.dataclass
class PublishedVersion(Entry):
    hub_id:     uuid.UUID
    dataset_id: uuid.UUID
    version:    int

    published_at: dt.datetime

    table_name = 'published_versions'
    primary_keys = ('hub_id', 'dataset_id', 'version')


@dc.dataclass
class Type(Entry):
    id: int

    name:      str

    table_name = 'types'
    primary_keys = ('id', )

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
    primary_keys = ('hub_id', 'dataset_id', 'version', 'name')


@dc.dataclass
class Partition(Entry):
    id: uuid.UUID

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
    primary_keys = ('id', )


@dc.dataclass
class PartitionStatus(Entry):
    partition_id: uuid.UUID

    status:     str
    updated_at: dt.datetime

    table_name = 'partition_statuses'
    primary_keys = ('partition_id', )


@dc.dataclass
class Connector(Entry):
    id: int

    name:     str
    config:   t.Dict[str, t.Any]
    template: str

    table_name = 'connectors'
    primary_keys = ('id', )


LocalPostgres = Connector(1, 'Local Postgres', {
    'host': 'localhost',
    'db': 'default',
    'user': 'postgres'
}, '''
psql -h '{{ host }}' -d '{{ db }}' -U '{{ user }}' -c 'SELECT * FROM {{ path }}'
''')

LocalMySQL = Connector(2, 'Local MySQL', {
    'host': 'localhost',
    'db': 'default',
    'user': 'mysql'
}, '''
mysql -h '{{ host }}' -u '{{ user }}' -p -e 'SELECT * FROM {{ path }}' {{ db }}
''')

Connectors = [
    LocalPostgres,
    LocalMySQL,
]


@dc.dataclass
class Connection(Entry):
    id: uuid.UUID

    hub_id:       uuid.UUID
    dataset_id:   uuid.UUID
    connector_id: int
    path:         str
    created_at:   dt.datetime

    table_name = 'connections'
    primary_keys = ('id', )


def write(cursor, entry):
    cursor.execute(f'INSERT INTO {entry.table_name} '
                   f'({", ".join(entry.columns)}) '
                   f'VALUES ({", ".join(["%s" for _ in entry.columns])})',
                   entry.values)


def upsert(cursor, entry):
    cursor.execute(f'INSERT INTO {entry.table_name} '
                   f'({", ".join(entry.columns)}) '
                   f'VALUES ({", ".join(["%s" for _ in entry.columns])}) '
                   f'ON CONFLICT ({", ".join(entry.primary_keys)}) '
                   f'DO UPDATE '
                   f'SET {", ".join([col + "=%s" for col in entry.secondary_columns])}',
                   entry.values + entry.secondary_values)
