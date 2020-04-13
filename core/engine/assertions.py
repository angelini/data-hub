import abc
import dataclasses as dc
import uuid

from core.engine import logging, security


class Assertion(abc.ABC):
    status_code: int

    def check(self, cursor):
        logging.info(f'check_{self.__class__.__name__}', self.__dict__)
        return self._check(cursor)

    @abc.abstractmethod
    def _check(self, cursor):
        pass

    @abc.abstractmethod
    def message(self):
        pass


@dc.dataclass
class NoOverlappingPartitions(Assertion):
    hub_id:     uuid.UUID
    dataset_id: uuid.UUID
    version:    int

    status_code = 400

    def _check(self, cursor):
        cursor.execute('''
            SELECT
                is_overlapping
            FROM
                dataset_versions
            WHERE
                hub_id = %s
            AND dataset_id = %s
            AND version = %s
        ''', (self.hub_id, self.dataset_id, self.version))
        if cursor.fetchone()[0]:
            return True

        cursor.execute('''
            SELECT
                id
            FROM
                partitions_with_last_end_time
            WHERE
                hub_id = %s
            AND dataset_id = %s
            AND version = %s
        ''', (self.hub_id, self.dataset_id, self.version))
        return cursor.rowcount == 0

    def message(self):
        return f'Version {self.hub_id}::{self.dataset_id}::{self.version} contains overlapping partitions'


@dc.dataclass
class TeamExists(Assertion):
    team_id: int

    status_code = 404

    def _check(self, cursor):
        cursor.execute('''
            SELECT 1
            FROM teams
            WHERE id = %s
        ''', (self.team_id, ))
        return cursor.rowcount == 1

    def message(self):
        return f'Team {self.team_id} does not exist'


@dc.dataclass
class DatasetExists(Assertion):
    dataset_id: uuid.UUID

    status_code = 404

    def _check(self, cursor):
        cursor.execute('''
            SELECT 1
            FROM datasets
            WHERE id = %s
        ''', (self.dataset_id, ))
        return cursor.rowcount == 1

    def message(self):
        return f'Dataset {self.dataset_id} does not exist'


@dc.dataclass
class VersionExists(Assertion):
    hub_id:     uuid.UUID
    dataset_id: uuid.UUID
    version:    int

    status_code = 404

    def _check(self, cursor):
        cursor.execute('''
            SELECT
                1
            FROM
                dataset_versions
            WHERE
                hub_id = %s
            AND dataset_id = %s
            AND version = %s
        ''', (self.hub_id, self.dataset_id, self.version))
        return cursor.rowcount == 1

    def message(self):
        return f'Version {self.hub_id}::{self.dataset_id}::{self.version} does not exist'


@dc.dataclass
class CorrectPassword(Assertion):
    email:    str
    password: str

    status_code = 403

    def _check(self, cursor):
        cursor.execute('''
            SELECT password_hash
            FROM users
            WHERE email = %s
        ''', (self.email, ))

        if cursor.rowcount == 0:
            return False

        hash = cursor.fetchone()[0]
        return security.pwd_context.verify(self.password, hash)

    def message(self):
        return f'Incorrect password for {self.email}'
