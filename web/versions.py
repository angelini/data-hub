import flask

from core.data import Backends, Types
from core.engine import DetailVersion, ListVersions, NewDatasetVersion
from web.db import read_view, write_action

bp = flask.Blueprint('versions', __name__, url_prefix='/hubs/<uuid:hub_id>/datasets/<uuid:dataset_id>/versions')


@bp.route('/index.json', methods=['GET'])
def versions_index_json(hub_id, dataset_id):
    return flask.jsonify(read_view(ListVersions(hub_id, dataset_id)))


@bp.route('/index.html', methods=['GET'])
def versions_index_html(hub_id, dataset_id):
    versions = read_view(ListVersions(hub_id, dataset_id))
    return flask.render_template('versions/index.html.j2',
                                 hub_id=hub_id,
                                 dataset_id=dataset_id,
                                 **versions)


@bp.route('/<int:version>.json', methods=['GET'])
def version_detail_json(hub_id, dataset_id, version):
    return flask.jsonify(read_view(DetailVersion(hub_id, dataset_id, version)))


@bp.route('/<int:version>.html', methods=['GET'])
def version_detail_html(hub_id, dataset_id, version):
    details = read_view(DetailVersion(hub_id, dataset_id, version))
    return flask.render_template('versions/detail.html.j2',
                                 hub_id=hub_id,
                                 dataset_id=dataset_id,
                                 version_int=version,
                                 **details)


@bp.route('/new.json', methods=['POST'])
def version_new_json(hub_id, dataset_id):
    data = flask.json()
    version_id = write_action(
        NewDatasetVersion(hub_id,
                          dataset_id,
                          data['backend'],
                          data['path'],
                          data['description'],
                          data['is_overlapping'],
                          data['columns'])
    )
    return flask.jsonify({'version_id': version_id})


@bp.route('/new.html', methods=['GET', 'POST'])
def version_new_html(hub_id, dataset_id):
    if flask.request.method == 'POST':
        data = flask.request.form
        columns = []
        for i in range(0, len(data.getlist('column_name[]'))):
            columns.append([
                data.getlist('column_name[]')[i],
                data.getlist('column_type[]')[i],
                data.getlist('column_description[]')[i],
                data.getlist('column_is_nullable[]')[i] == 'true',
                data.getlist('column_is_unique[]')[i] == 'true',
                data.getlist('column_has_pii[]')[i] == 'true',
            ])

        write_action(
            NewDatasetVersion(hub_id,
                              dataset_id,
                              data['backend'],
                              data['path'],
                              data['description'],
                              bool(data.get('is_overlapping')),
                              columns)
        )
        return flask.redirect(flask.url_for('versions.versions_index_html', hub_id=hub_id, dataset_id=dataset_id))

    return flask.render_template('versions/new.html.j2',
                                 hub_id=hub_id,
                                 dataset_id=dataset_id,
                                 backends=Backends,
                                 types=Types)
