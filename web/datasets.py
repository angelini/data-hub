import flask

from core.engine import ListDatasets, NewDataset
from web.db import DbException, read_view, write_action

bp = flask.Blueprint('datasets', __name__, url_prefix='/hubs/<uuid:hub_id>/datasets')


@bp.route('/index.json', methods=['GET'])
def datasets_index_json(hub_id):
    return flask.jsonify(read_view(ListDatasets(hub_id)))


@bp.route('/index.html', methods=['GET'])
def datasets_index_html(hub_id):
    return flask.render_template('datasets/index.html.j2',
                                 hub_id=hub_id,
                                 **read_view(ListDatasets(hub_id)))


@bp.route('/new.json', methods=['POST'])
def dataset_new_json(hub_id):
    data = flask.json()
    dataset_id = write_action(NewDataset(hub_id, data['name']))
    return flask.jsonify({'dataset_id': dataset_id})


@bp.route('/new.html', methods=['GET', 'POST'])
def dataset_new_html(hub_id):
    error = None

    if flask.request.method == 'POST':
        data = flask.request.form
        try:
            write_action(NewDataset(hub_id, data['name']))
            return flask.redirect(flask.url_for('datasets.datasets_index_html', hub_id=hub_id))
        except DbException as e:
            error = str(e)

    return flask.render_template('datasets/new.html.j2', hub_id=hub_id, error=error)
