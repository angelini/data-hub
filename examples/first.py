import datetime as dt
import json
import random

import requests

from core.data import FileBackend, IntType, StringType


class DateTimeEncoder(json.JSONEncoder):

    def default(self, o):
        if isinstance(o, dt.datetime):
            return o.isoformat()

        return json.JSONEncoder.default(self, o)


HOST = 'localhost'
PORT = 5000


def post(path, data):
    if not path.startswith('/'):
        path = '/' + path
    response = requests.post(f'http://{HOST}:{PORT}{path}',
                             data=json.dumps(data, cls=DateTimeEncoder),
                             headers={'Content-type': 'application/json'})
    if response.status_code >= 400:
        print(response.text)
    response.raise_for_status()
    return response.json()


def new_user(email, password):
    return post(
        'users/new.json',
        {'email': email, 'password': password},
    )['user_id']

def new_team(name):
    return post(
        'teams/new.json',
        {'name': name},
    )['team_id']


def new_team_member(team_id, user_id):
    return post(
        f'teams/{team_id}/new_member.json',
        {'user_id': user_id},
    )['member_id']

def new_hub(name, team_id):
    return post(
        'hubs/new.json',
        {'name': name, 'team_id': team_id},
    )['hub_id']


def new_dataset(hub_id, name):
    return post(
        f'hubs/{hub_id}/datasets/new.json',
        {'name': name},
    )['dataset_id']


def new_version(hub_id, dataset_id, backend, path, keys, description, is_overlapping, columns, depends_on):
    return post(
        f'hubs/{hub_id}/datasets/{dataset_id}/versions/new.json',
        {'backend': backend, 'path': path, 'partition_keys': keys,
         'description': description, 'is_overlapping': is_overlapping,
         'columns': columns, 'depends_on': depends_on},
    )['version']


def publish_version(hub_id, dataset_id, version):
    return post(f'hubs/{hub_id}/datasets/{dataset_id}/versions/{version}/publish.json', {})


def new_partition(hub_id, dataset_id, version, path, values, count, start, end):
    return post(
        f'hubs/{hub_id}/datasets/{dataset_id}/versions/{version}/partitions/new.json',
        {'path': path, 'partition_values': values,
         'row_count': count, 'start_time': start, 'end_time': end},
    )['partition_id']


def build_full_dataset(hub_id, dataset_name, columns, version_count=5, depends_on=None):
    depends_on = depends_on or []
    dataset_id = new_dataset(hub_id, dataset_name)

    versions = [
        new_version(
            hub_id, dataset_id,
            FileBackend.module, f'/tables/{dataset_name}/{i}', ['day', 'country'],
            '', False,
            columns,
            depends_on,
        )
        for i in range(1, 6)
    ]

    for version in versions:
        duration = random.randint(4, 20)
        today = dt.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        start = today - dt.timedelta(days=(duration + random.randint(0, 10)))

        for day_increment in range(duration):
            start = start + dt.timedelta(days=day_increment)
            end = start + dt.timedelta(days=1)
            for country in ['CA', 'US']:
                new_partition(
                    hub_id, dataset_id, version,
                    f'/tables/{dataset_name}/{version}', [start.strftime('%Y-%m-%d'), country],
                    random.randint(10, 2000), start, end
                )

    published_version = versions[random.randint(-1 * version_count, -1)]
    publish_version(hub_id, dataset_id, published_version)

    return (hub_id, dataset_id, published_version)


def main(prefix=''):
    user_id = new_user(f'{prefix}default@local', 'pass')
    new_user(f'{prefix}other@local', 'pass')

    team_id = new_team(f'{prefix}Everyone')
    new_team_member(team_id, user_id)

    marketing_hub_id = new_hub(f'{prefix}Marketing', team_id)

    visits = build_full_dataset(
        marketing_hub_id,
        'visits',
        [
            ('id', IntType.name, '', False, True, False),
            ('email', StringType.name, '', False, True, True),
            ('time', IntType.name, '', False, False, False),
        ]
    )

    leads = build_full_dataset(
        marketing_hub_id,
        'leads',
        [
            ('id', IntType.name, '', False, True, False),
            ('email', StringType.name, '', False, True, True),
        ],
        depends_on=[visits]
    )

    campaigns = build_full_dataset(
        marketing_hub_id,
        'campaigns',
        [
            ('id', IntType.name, '', False, True, False),
            ('name', StringType.name, '', False, False, False),
            ('price', IntType.name, '', False, False, False),
        ],
        depends_on=[leads]
    )

    build_full_dataset(
        marketing_hub_id,
        'conversions',
        [
            ('id', IntType.name, '', False, True, False),
            ('lead_id', IntType.name, '', False, False, False),
            ('customer_id', IntType.name, '', False, False, False),
        ],
        depends_on=[leads, campaigns]
    )

    sales_hub_id = new_hub(f'{prefix}Sales', team_id)

    customers = build_full_dataset(
        sales_hub_id,
        'customers',
        [
            ('id', IntType.name, '', False, True, False),
            ('email', StringType.name, '', False, True, True),
            ('first_name', StringType.name, '', False, False, False),
            ('last_name', StringType.name, '', False, False, True),
        ]
    )

    build_full_dataset(
        sales_hub_id,
        'orders',
        [
            ('id', IntType.name, '', False, True, False),
            ('price', IntType.name, '', False, False, False),
            ('time', IntType.name, '', False, False, False),
        ],
        depends_on=[customers]
    )

    finance_hub_id = new_hub(f'{prefix}Finance', team_id)

    build_full_dataset(
        finance_hub_id,
        'expenses',
        [
            ('id', IntType.name, '', False, True, False),
            ('price', IntType.name, '', False, False, False),
            ('time', IntType.name, '', False, False, False),
        ]
    )


if __name__ == '__main__':
    main()
