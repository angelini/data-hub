import abc
import dataclasses as dc
import datetime as dt
import pathlib as pl
import uuid
import typing as t

import pytz

from core.data import AccessLevel, Backend, Column, Connection, Dataset, DatasetVersion, Dependency, Hub, \
    Partition, PartitionStatus, PublishedVersion, Status, Team, TeamMember, TeamRole, Type, User, \
    write, upsert
from core.engine import logging, security


class Action(abc.ABC):

    def execute(self, cursor):
        logging.info(f'execute_{self.__class__.__name__}', self.__dict__)
        return self._execute(cursor)

    @abc.abstractmethod
    def _execute(self, cursor):
        pass


@dc.dataclass
class NewUser(Action):
    email:    str
    password: str

    def _execute(self, cursor):
        user_id = uuid.uuid4()
        password_hash = security.pwd_context.hash(self.password)
        write(cursor, User(user_id, self.email, password_hash, dt.datetime.now(tz=pytz.utc)))
        return user_id


@dc.dataclass
class NewTeam(Action):
    name: str

    def _execute(self, cursor):
        team_id = uuid.uuid4()
        write(cursor, Team(team_id, self.name, dt.datetime.now(tz=pytz.utc)))
        return team_id


@dc.dataclass
class NewTeamMember(Action):
    team_id: uuid.UUID
    user_id: uuid.UUID

    def _execute(self, cursor):
        member_id = uuid.uuid4()
        write(cursor, TeamMember(member_id, self.team_id, self.user_id, dt.datetime.now(tz=pytz.utc), None))
        return member_id


@dc.dataclass
class NewHub(Action):
    team_id: uuid.UUID
    name:    str

    def _execute(self, cursor):
        created_at = dt.datetime.now(tz=pytz.utc)

        hub_id = uuid.uuid4()
        write(cursor, Hub(hub_id, self.team_id, self.name, created_at))

        team_role_id = uuid.uuid4()
        write(cursor, TeamRole(team_role_id, self.team_id, hub_id, AccessLevel.ADMIN.value, created_at))

        return hub_id


@dc.dataclass
class NewDataset(Action):
    hub_id: uuid.UUID
    name:   str

    def _execute(self, cursor):
        dataset_id = uuid.uuid4()
        write(cursor, Dataset(self.hub_id, dataset_id, self.name, dt.datetime.now(tz=pytz.utc), None))
        return dataset_id


@dc.dataclass
class NewDatasetVersion(Action):
    hub_id:         uuid.UUID
    dataset_id:     uuid.UUID
    backend:        str
    path:           pl.Path
    partition_keys: t.List[str]
    description:    str
    is_overlapping: bool
    columns:        t.List[t.Tuple[str, str, str, bool, bool, bool]]
    depends_on:     t.List[t.Tuple[str, str, int]]

    def _execute(self, cursor):
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
                                     self.partition_keys,
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

        for (parent_hub_id, parent_dataset_id, parent_version) in self.depends_on:
            write(cursor, Dependency(parent_hub_id,
                                     parent_dataset_id,
                                     parent_version,
                                     self.hub_id,
                                     self.dataset_id,
                                     latest_version + 1))

        return latest_version + 1


@dc.dataclass
class NewPartition(Action):
    hub_id:     uuid.UUID
    dataset_id: uuid.UUID
    version:    int
    path:       str
    values:     t.List[str]
    row_count:  t.Optional[int]
    start_time: t.Optional[dt.datetime]
    end_time:   t.Optional[dt.datetime]

    def _execute(self, cursor):
        partition_id = uuid.uuid4()
        write(cursor, Partition(partition_id,
                                self.hub_id,
                                self.dataset_id,
                                self.version,
                                self.path,
                                self.values,
                                self.row_count,
                                self.start_time,
                                self.end_time,
                                dt.datetime.now(tz=pytz.utc),
                                None))
        return partition_id


@dc.dataclass
class NewConnection(Action):
    hub_id:       uuid.UUID
    dataset_id:   uuid.UUID
    connector_id: uuid.UUID
    path:         str

    def _execute(self, cursor):
        connection_id = uuid.uuid4()
        write(cursor, Connection(connection_id,
                                 self.hub_id,
                                 self.dataset_id,
                                 self.connector_id,
                                 self.path,
                                 dt.datetime.now(tz=pytz.utc)))
        return connection_id


@dc.dataclass
class PublishVersion(Action):
    hub_id:     uuid.UUID
    dataset_id: uuid.UUID
    version:    int

    def _execute(self, cursor):
        write(cursor, PublishedVersion(self.hub_id,
                                       self.dataset_id,
                                       self.version,
                                       dt.datetime.now(tz=pytz.utc)))


@dc.dataclass
class SetQueuedPartitionStatus(Action):
    hub_id:     uuid.UUID
    dataset_id: uuid.UUID
    version:    int

    def _execute(self, cursor):
        cursor.execute('''
            SELECT id
            FROM partitions
            WHERE
                hub_id = %s
            AND dataset_id = %s
            AND version = %s
        ''', (self.hub_id, self.dataset_id, self.version))
        for row in cursor.fetchall():
            upsert(cursor, PartitionStatus(row[0],
                                           Status.QUEUED.value,
                                           dt.datetime.now(tz=pytz.utc)))


@dc.dataclass
class UpdatePartitionStatus(Action):
    partition_id: uuid.UUID
    status:       Status

    def _execute(self, cursor):
        upsert(cursor, PartitionStatus(self.partition_id,
                                       self.status.value,
                                       dt.datetime.now(tz=pytz.utc)))
