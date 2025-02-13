import datetime as dt
import json
import pathlib
import random
import shutil

import requests

from core.data import FileBackend, IntType, StringType


class DateTimeEncoder(json.JSONEncoder):

    def default(self, o):
        if isinstance(o, dt.datetime):
            return o.isoformat()

        return json.JSONEncoder.default(self, o)


HOST = 'localhost'
PORT = 5000
EMAIL = 'default@local'
PASSWORD = 'pass'
TMP_DIR = pathlib.Path('/tmp/tables')


def get_access_token_builder():
    memoized = {}

    def get():
        if 'access_token' not in memoized:
            response = requests.post(f'http://{HOST}:{PORT}/auth/login.json',
                                     headers={'Content-type': 'application/json'},
                                     json={'email': EMAIL, 'password': PASSWORD})
            response.raise_for_status()
            memoized['access_token'] = response.json()['access_token']
        return memoized['access_token']

    return get


get_access_token = get_access_token_builder()


def post(path, data, auth=True):
    if not path.startswith('/'):
        path = '/' + path

    headers = {'Content-type': 'application/json'}
    if auth:
        headers['Authorization'] = f'Bearer {get_access_token()}'

    response = requests.post(f'http://{HOST}:{PORT}{path}',
                             data=json.dumps(data, cls=DateTimeEncoder),
                             headers=headers)

    if response.status_code >= 400:
        print(response.text)
    response.raise_for_status()

    return response.json()


def new_user(email, password):
    return post(
        'users/new.json',
        {'email': email, 'password': password},
        auth=False
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
    global get_access_token

    hub_id = post(
        'hubs/new.json',
        {'name': name, 'team_id': team_id},
    )['hub_id']

    # Refresh the auth token after creating a new hub
    get_access_token = get_access_token_builder()

    return hub_id


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

    dataset_path = TMP_DIR.joinpath(f'{dataset_name}')
    versions = [
        new_version(
            hub_id, dataset_id,
            FileBackend.module, str(dataset_path.joinpath(str(i))), ['country', 'day'],
            '', False,
            columns,
            depends_on,
        )
        for i in range(1, 6)
    ]

    for version in versions:
        version_path = dataset_path.joinpath(str(version))
        duration = random.randint(4, 20)
        today = dt.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        start = today - dt.timedelta(days=(duration + random.randint(0, 10)))

        for day_increment in range(duration):
            part_start = start + dt.timedelta(days=day_increment)
            part_end = part_start + dt.timedelta(days=1)
            part_day = part_start.strftime("%Y-%m-%d")

            for country in ['CA', 'US']:
                part_path = str(version_path.joinpath(f'country={country}/day={part_day}'))
                new_partition(
                    hub_id, dataset_id, version, part_path,
                    [country, part_day],
                    random.randint(10, 2000), part_start, part_end
                )
                pathlib.Path(part_path).mkdir(parents=True, exist_ok=True)

    published_version = versions[random.randint(-1 * version_count, -1)]
    publish_version(hub_id, dataset_id, published_version)

    return (hub_id, dataset_id, published_version)


def main():
    if TMP_DIR.exists():
        shutil.rmtree(TMP_DIR)

    user_id = new_user(EMAIL, PASSWORD)
    new_user('other@local', PASSWORD)

    team_id = new_team('Everyone')
    new_team_member(team_id, user_id)

    marketing_hub_id = new_hub('Marketing', team_id)

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

    sales_hub_id = new_hub('Sales', team_id)

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

    finance_hub_id = new_hub('Finance', team_id)

    build_full_dataset(
        finance_hub_id,
        'expenses',
        [
            ('id', IntType.name, '', False, True, False),
            ('customer_id', IntType.name, '', False, True, False),
            ('price', IntType.name, '', False, False, False),
            ('time', IntType.name, '', False, False, False),
        ],
        depends_on=[customers]
    )


if __name__ == '__main__':
    main()
