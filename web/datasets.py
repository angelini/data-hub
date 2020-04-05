import flask

from core.engine import ListDatasets, NewDataset
from web.auth import can_read_current_hub, auth_write_current_hub
from web.db import DbException, fetch_view, execute_action

bp = flask.Blueprint('datasets', __name__, url_prefix='/hubs/<uuid:hub_id>/datasets')


@bp.before_request
def authorize_before_request():
    return can_read_current_hub()


@bp.route('/index.json', methods=['GET'])
def index_json(hub_id):
    return flask.jsonify(fetch_view(ListDatasets(hub_id)))


@bp.route('/index.html', methods=['GET'])
def index_html(hub_id):
    return flask.render_template('datasets/index.html.j2',
                                 hub_id=hub_id,
                                 **fetch_view(ListDatasets(hub_id)))


@bp.route('/new.json', methods=['POST'])
@auth_write_current_hub
def new_json(hub_id):
    data = flask.request.json
    dataset_id = execute_action(NewDataset(hub_id, data['name']))
    return flask.jsonify({'dataset_id': dataset_id}), 201


@bp.route('/new.html', methods=['GET', 'POST'])
@auth_write_current_hub
def new_html(hub_id):
    error = None

    if flask.request.method == 'POST':
        data = flask.request.form
        try:
            execute_action(NewDataset(hub_id, data['name']))
            return flask.redirect(flask.url_for('datasets.index_html', hub_id=hub_id))
        except DbException as e:
            error = str(e)

    return flask.render_template('datasets/new.html.j2', hub_id=hub_id, error=error)
