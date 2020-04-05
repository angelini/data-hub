import datetime as dt

import flask

from core.engine import NewPartition, VersionExists
from web.auth import can_read_current_hub, auth_write_current_hub
from web.db import check_assertion, execute_action

bp = flask.Blueprint('partitions', __name__,
                     url_prefix='/hubs/<uuid:hub_id>/datasets/<uuid:dataset_id>/versions/<int:version>/partitions')


@bp.before_request
def authorize_before_request():
    return can_read_current_hub()


@bp.route('/new.json', methods=['POST'])
@auth_write_current_hub
def new_json(hub_id, dataset_id, version):
    check_assertion(VersionExists(hub_id, dataset_id, version))
    data = flask.request.json
    partition_id = execute_action(
        NewPartition(hub_id,
                     dataset_id,
                     version,
                     data['path'],
                     data['partition_values'],
                     data.get('row_count'),
                     data.get('start_time'),
                     data.get('end_time'))
    )
    return flask.jsonify({'partition_id': partition_id}), 201


@bp.route('/new.html', methods=['GET', 'POST'])
@auth_write_current_hub
def new_html(hub_id, dataset_id, version):
    check_assertion(VersionExists(hub_id, dataset_id, version))
    if flask.request.method == 'POST':
        data = flask.request.form
        start_time, end_time = None, None

        if data.get('start_date'):
            if data.get('start_time'):
                start_time = dt.datetime.strptime(f'{data["start_date"]} {data["start_time"]}', '%Y-%m-%d %H:%M')
            else:
                start_time = dt.datetime.strptime(data['start_date'], '%Y-%m-%d')

        if data.get('end_date'):
            if data.get('end_time'):
                end_time = dt.datetime.strptime(f'{data["end_date"]} {data["end_time"]}', '%Y-%m-%d %H:%M')
            else:
                end_time = dt.datetime.strptime(data['end_date'], '%Y-%m-%d')

        execute_action(
            NewPartition(hub_id,
                         dataset_id,
                         version,
                         data.getlist('partition_values[]'),
                         data['path'],
                         data.get('row_count'),
                         start_time,
                         end_time)
        )
        return flask.redirect(flask.url_for('versions.detail_html',
                                            hub_id=hub_id, dataset_id=dataset_id, version=version))

    return flask.render_template('partitions/new.html.j2',
                                 hub_id=hub_id,
                                 dataset_id=dataset_id,
                                 version=version)
