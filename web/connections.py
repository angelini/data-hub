import flask

from core.engine import DatasetExists, NewConnection
from web.auth import auth_current_hub_reader, require_writer
from web.db import check_assertion, execute_action

bp = flask.Blueprint('connections', __name__, url_prefix='/hubs/<uuid:hub_id>/datasets/<uuid:dataset_id>/connections')


@bp.before_request
def authorize_before_request():
    return auth_current_hub_reader()


@bp.route('/new.json', methods=['POST'])
@require_writer
def new_json(hub_id, dataset_id):
    check_assertion(DatasetExists(dataset_id))
    data = flask.request.json
    connection_id = execute_action(
        NewConnection(hub_id,
                      dataset_id,
                      data['connector_id'],
                      data['path'])
    )
    return flask.jsonify({'connection_id': connection_id}), 201


@bp.route('/new.html', methods=['POST'])
@require_writer
def new_html(hub_id, dataset_id):
    check_assertion(DatasetExists(dataset_id))
    data = flask.request.form

    execute_action(NewConnection(hub_id,
                                 dataset_id,
                                 data['connector_id'],
                                 data['path']))

    return flask.redirect(flask.url_for('versions.index_html',
                                        hub_id=hub_id, dataset_id=dataset_id))
