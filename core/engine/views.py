import abc
import collections as cl
import dataclasses as dc
import uuid

import jinja2 as jinja

from core.data import AccessLevel
from core.engine import logging


class View(abc.ABC):

    def fetch(self, cursor):
        logging.info(f'fetch_{self.__class__.__name__}', self.__dict__)
        return self._fetch(cursor)

    @abc.abstractmethod
    def _fetch(self, cursor):
        pass


@dc.dataclass
class ListUsers(View):

    def _fetch(self, cursor):
        cursor.execute('''
            SELECT id, email, created_at
            FROM users
            ORDER BY created_at
        ''')
        return {
            'users': [
                {
                    'id': row[0],
                    'email': row[1],
                    'created_at': row[2],
                }
                for row in cursor.fetchall()
            ]
        }


@dc.dataclass
class ListTeams(View):

    def _fetch(self, cursor):
        cursor.execute('''
            SELECT id, name, created_at
            FROM teams
            ORDER BY created_at
        ''')
        return {
            'teams': [
                {
                    'id': row[0],
                    'name': row[1],
                    'created_at': row[2],
                }
                for row in cursor.fetchall()
            ]
        }


@dc.dataclass
class ListHubs(View):

    def _fetch(self, cursor):
        cursor.execute('''
            SELECT id, name, team_id, team_name, created_at
            FROM hubs_with_team_name
            ORDER BY created_at
        ''')
        return {
            'hubs': [
                {
                    'id': row[0],
                    'name': row[1],
                    'team_id': row[2],
                    'team_name': row[3],
                    'created_at': row[4],
                }
                for row in cursor.fetchall()
            ]
        }


@dc.dataclass
class ListDatasets(View):
    hub_id: uuid.UUID

    def _fetch(self, cursor):
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
class ListBackends(View):

    def _fetch(self, cursor):
        cursor.execute('''
            SELECT id, module
            FROM backends
        ''')
        return [{
            'id': row[0],
            'module': row[1],

        } for row in cursor.fetchall()]


@dc.dataclass
class ListVersions(View):
    hub_id:     uuid.UUID
    dataset_id: uuid.UUID

    def _fetch(self, cursor):
        cursor.execute('''
            SELECT
                ver.version,
                ver.module,
                ver.path,
                ver.description,
                ver.created_at,
                CASE WHEN cur.published_at IS NULL THEN false
                     ELSE true
                     END AS published
            FROM
                versions_with_backend ver
            LEFT JOIN
                current_published_versions cur
            ON
                ver.hub_id = cur.hub_id
            AND ver.dataset_id = cur.dataset_id
            AND ver.version = cur.version
            WHERE
                ver.hub_id = %s
            AND ver.dataset_id = %s
            ORDER BY ver.created_at
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
                    'published': row[5],
                }
                for row in cursor.fetchall()
            ]
        }


@dc.dataclass
class ListPartitions(View):
    hub_id:     uuid.UUID
    dataset_id: uuid.UUID
    version:    int

    def _fetch(self, cursor):
        cursor.execute('''
            SELECT id, path
            FROM partitions
            WHERE
                hub_id = %s
            AND dataset_id = %s
            AND version = %s
            ORDER BY created_at DESC
        ''', (self.hub_id, self.dataset_id, self.version))
        return {
            'partitions': [
                {
                    'id': row[0],
                    'path': row[1],
                }
                for row in cursor.fetchall()
            ]
        }


@dc.dataclass
class DetailUser(View):
    email: str

    def _fetch(self, cursor):
        cursor.execute('''
            SELECT id
            FROM users
            WHERE email = %s
        ''', (self.email, ))
        user_id = cursor.fetchone()[0]

        cursor.execute('''
            WITH user_teams AS (
                SELECT team_id
                FROM team_members
                WHERE user_id = %s
            )
            SELECT
                rol.hub_id,
                rol.access_level
            FROM
                current_team_roles_with_name rol
            INNER JOIN
                team_members tms
            ON
                rol.team_id = tms.team_id
        ''', (user_id, ))
        roles = {}
        for row in cursor.fetchall():
            hub_id = str(row[0])
            if hub_id in roles:
                roles[hub_id] = AccessLevel.highest_level(roles[hub_id], row[1])
            else:
                roles[hub_id] = row[1]

        return {
            'user_id': user_id,
            'roles': roles,
        }


@dc.dataclass
class DetailTeam(View):
    team_id: uuid.UUID

    def _fetch(self, cursor):
        cursor.execute('''
            SELECT
                user_id,
                user_email,
                created_at
            FROM
                current_team_members_with_email
            WHERE
                team_id = %s
        ''', (self.team_id, ))
        members = [{
            'id': row[0],
            'email': row[1],
            'created_at': row[2],
        } for row in cursor.fetchall()]

        cursor.execute('''
            SELECT
                hub_id,
                hub_name,
                access_level
            FROM
                current_team_roles_with_name
            WHERE
                team_id = %s
        ''', (self.team_id, ))
        roles = [{
            'hub_id': row[0],
            'hub_name': row[1],
            'access_level': row[2],
        } for row in cursor.fetchall()]

        cursor.execute('''
            SELECT name
            FROM teams
            WHERE id = %s
        ''', (self.team_id, ))
        row = cursor.fetchone()

        member_ids = [user['id'] for user in members]
        users = [user
                 for user in ListUsers()._fetch(cursor)['users']
                 if user['id'] not in member_ids]

        return {
            'name': row[0],
            'members': members,
            'roles': roles,
            'users': users,
        }


@dc.dataclass
class DetailHub(View):
    hub_id: uuid.UUID

    def _fetch(self, cursor):
        cursor.execute('''
            SELECT name
            FROM hubs
            WHERE id = %s
        ''', (self.hub_id, ))
        row = cursor.fetchone()
        return {
            'name': row[0]
        }


@dc.dataclass
class DetailDataset(View):
    hub_id:     uuid.UUID
    dataset_id: uuid.UUID

    def _fetch(self, cursor):
        cursor.execute('''
            SELECT id, name
            FROM connectors
            ORDER BY id
        ''')
        connectors = [{
            'id': row[0],
            'name': row[1],
        } for row in cursor.fetchall()]

        cursor.execute('''
            SELECT
                id,
                connector_id,
                connector_name,
                path
            FROM
                connections_with_connector
            WHERE
                hub_id = %s
            AND dataset_id = %s
            ORDER BY
                created_at DESC
        ''', (self.hub_id, self.dataset_id))
        connections = [{
            'id': row[0],
            'connector_id': row[1],
            'connector_name': row[2],
            'path': row[3],
        } for row in cursor.fetchall()]

        cursor.execute('''
            SELECT name
            FROM datasets
            WHERE id = %s
        ''', (self.dataset_id, ))
        row = cursor.fetchone()
        return {
            'name': row[0],
            'connectors': connectors,
            'connections': connections,
        }


@dc.dataclass
class DetailVersion(View):
    hub_id:     uuid.UUID
    dataset_id: uuid.UUID
    version:    int

    def _fetch(self, cursor):
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
            SELECT
                partition_values,
                path,
                row_count,
                start_time,
                end_time,
                created_at,
                status,
                updated_at
            FROM
                partitions_with_status
            WHERE
                hub_id = %s
            AND dataset_id = %s
            AND version = %s
            ORDER BY end_time, start_time DESC
        ''', (self.hub_id, self.dataset_id, self.version))
        partitions = [{
            'partition_values': row[0],
            'path': row[1],
            'row_count': row[2],
            'start_time': row[3],
            'end_time': row[4],
            'created_at': row[5],
            'status': row[6],
            'updated_at': row[7],
        } for row in cursor.fetchall()]

        cursor.execute('''
            WITH RECURSIVE children AS (
                SELECT
                    parent_hub_id,
                    parent_dataset_id,
                    parent_version,
                    child_hub_id,
                    child_dataset_id,
                    child_version
                FROM dependencies
                WHERE
                    parent_hub_id = %s
                AND parent_dataset_id = %s
                AND parent_version = %s
                UNION
                SELECT
                    dep.parent_hub_id,
                    dep.parent_dataset_id,
                    dep.parent_version,
                    dep.child_hub_id,
                    dep.child_dataset_id,
                    dep.child_version
                FROM
                    dependencies AS dep
                INNER JOIN
                    children AS chi
                ON
                    dep.parent_hub_id = chi.child_hub_id
                AND dep.parent_dataset_id = chi.child_dataset_id
                AND dep.parent_version = chi.child_version
            )
            SELECT
                chi.parent_hub_id,
                phub.name AS parent_hub_name,
                chi.parent_dataset_id,
                pdat.name AS parent_dataset_name,
                chi.parent_version,
                chi.child_hub_id,
                chub.name AS child_hub_name,
                chi.child_dataset_id,
                cdat.name AS child_dataset_name,
                chi.child_version
            FROM
                children chi
            INNER JOIN
                hubs phub
            ON
                chi.parent_hub_id = phub.id
            INNER JOIN
                hubs chub
            ON
                chi.child_hub_id = chub.id
            INNER JOIN
                datasets pdat
            ON
                chi.parent_dataset_id = pdat.id
            INNER JOIN
                datasets cdat
            ON
                chi.child_dataset_id = cdat.id
        ''', (self.hub_id, self.dataset_id, self.version))
        dependencies = [{
            'parent': {
                'hub_id': row[0],
                'hub_name': row[1],
                'dataset_id': row[2],
                'dataset_name': row[3],
                'version': row[4],
                'key': f'{row[0]}:{row[2]}:{row[4]}',
                'is_same_hub': row[0] == self.hub_id,
                'is_selected': row[0] == self.hub_id and row[2] == self.dataset_id and row[4] == self.version,
            },
            'child': {
                'hub_id': row[5],
                'hub_name': row[6],
                'dataset_id': row[7],
                'dataset_name': row[8],
                'version': row[9],
                'key': f'{row[5]}:{row[7]}:{row[9]}',
                'is_same_hub': row[5] == self.hub_id,
                'is_selected': row[5] == self.hub_id and row[7] == self.dataset_id and row[9] == self.version,
            }
        } for row in cursor.fetchall()]

        cursor.execute('''
            WITH RECURSIVE parents AS (
                SELECT
                    parent_hub_id,
                    parent_dataset_id,
                    parent_version,
                    child_hub_id,
                    child_dataset_id,
                    child_version
                FROM dependencies
                WHERE
                    child_hub_id = %s
                AND child_dataset_id = %s
                AND child_version = %s
                UNION
                SELECT
                    dep.parent_hub_id,
                    dep.parent_dataset_id,
                    dep.parent_version,
                    dep.child_hub_id,
                    dep.child_dataset_id,
                    dep.child_version
                FROM
                    dependencies AS dep
                INNER JOIN
                    parents AS par
                ON
                    dep.child_hub_id = par.parent_hub_id
                AND dep.child_dataset_id = par.parent_dataset_id
                AND dep.child_version = par.parent_version
            )
            SELECT
                par.parent_hub_id,
                phub.name AS parent_hub_name,
                par.parent_dataset_id,
                pdat.name AS parent_dataset_name,
                par.parent_version,
                par.child_hub_id,
                chub.name AS child_hub_name,
                par.child_dataset_id,
                cdat.name AS child_dataset_name,
                par.child_version
            FROM
                parents par
            INNER JOIN
                hubs phub
            ON
                par.parent_hub_id = phub.id
            INNER JOIN
                hubs chub
            ON
                par.child_hub_id = chub.id
            INNER JOIN
                datasets pdat
            ON
                par.parent_dataset_id = pdat.id
            INNER JOIN
                datasets cdat
            ON
                par.child_dataset_id = cdat.id
        ''', (self.hub_id, self.dataset_id, self.version))
        dependencies.extend([{
            'parent': {
                'hub_id': row[0],
                'hub_name': row[1],
                'dataset_id': row[2],
                'dataset_name': row[3],
                'version': row[4],
                'key': f'{row[0]}:{row[2]}:{row[4]}',
                'is_same_hub': row[0] == self.hub_id,
                'is_selected': row[0] == self.hub_id and row[2] == self.dataset_id and row[4] == self.version,
            },
            'child': {
                'hub_id': row[5],
                'hub_name': row[6],
                'dataset_id': row[7],
                'dataset_name': row[8],
                'version': row[9],
                'key': f'{row[5]}:{row[7]}:{row[9]}',
                'is_same_hub': row[5] == self.hub_id,
                'is_selected': row[5] == self.hub_id and row[7] == self.dataset_id and row[9] == self.version,
            }
        } for row in cursor.fetchall()])

        cursor.execute('''
            SELECT partition_keys, module, path, description, created_at
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
                'partition_keys': row[0],
                'module': row[1],
                'path': row[2],
                'description': row[3],
                'created_at': row[4],
            },
            'columns': columns,
            'partitions': partitions,
            'dependencies': dependencies,
        }


@dc.dataclass
class VersionBackendId(View):
    hub_id:     uuid.UUID
    dataset_id: uuid.UUID
    version:    int

    def _fetch(self, cursor):
        cursor.execute('''
            SELECT backend_id
            FROM dataset_versions
            WHERE
                hub_id = %s
            AND dataset_id = %s
            AND version = %s
        ''', (self.hub_id, self.dataset_id, self.version))
        return cursor.fetchone()[0]


@dc.dataclass
class RenderConnection(View):
    connection_id: uuid.UUID

    def _fetch(self, cursor):
        cursor.execute('''
            SELECT connector_config, connector_template, path
            FROM connections_with_connector
            WHERE id = %s
        ''', (self.connection_id, ))
        row = cursor.fetchone()
        return {
            'connection': jinja.Template(row[1]).render(path=row[2], **row[0]),
        }


@dc.dataclass
class PublishedVersions(View):

    def _fetch(self, cursor):
        published = cl.defaultdict(lambda: cl.defaultdict(list))

        cursor.execute('''
            SELECT hub_id, hub_name, dataset_id, dataset_name, version
            FROM current_published_versions_with_names;
        ''')
        for row in cursor.fetchall():
            published[f'{row[0]}:{row[1]}'][f'{row[2]}:{row[3]}'].append(row[4])

        return published
