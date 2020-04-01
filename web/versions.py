import flask

from core.data import Backends, Types
from core.engine import DetailVersion, ListVersions, NewDatasetVersion, PublishedVersions, PublishVersion, VersionExists
from web.db import DbException, check_assertion, fetch_view, execute_action

bp = flask.Blueprint('versions', __name__, url_prefix='/hubs/<uuid:hub_id>/datasets/<uuid:dataset_id>/versions')


@bp.route('/index.json', methods=['GET'])
def versions_index_json(hub_id, dataset_id):
    return flask.jsonify(fetch_view(ListVersions(hub_id, dataset_id)))


@bp.route('/index.html', methods=['GET'])
def versions_index_html(hub_id, dataset_id):
    versions = fetch_view(ListVersions(hub_id, dataset_id))
    return flask.render_template('versions/index.html.j2',
                                 hub_id=hub_id,
                                 dataset_id=dataset_id,
                                 **versions)


@bp.route('/new.json', methods=['POST'])
def version_new_json(hub_id, dataset_id):
    data = flask.request.json
    version_id = execute_action(
        NewDatasetVersion(hub_id,
                          dataset_id,
                          data['backend'],
                          data['path'],
                          data['partition_keys'],
                          data['description'],
                          data['is_overlapping'],
                          data['columns'],
                          data['depends_on'])
    )
    return flask.jsonify({'version': version_id}), 201


@bp.route('/new.html', methods=['GET', 'POST'])
def version_new_html(hub_id, dataset_id):
    error = None

    if flask.request.method == 'POST':
        data = flask.request.form
        columns = [
            [
                name,
                data.getlist('column_type[]')[idx],
                data.getlist('column_description[]')[idx],
                data.getlist('column_is_nullable[]')[idx] == 'true',
                data.getlist('column_is_unique[]')[idx] == 'true',
                data.getlist('column_has_pii[]')[idx] == 'true',
            ]
            for (idx, name) in enumerate(data.getlist('column_name[]'))
        ]
        depends_on = [
            [
                hub_id,
                data.getlist('parent_dataset_id[]')[idx],
                int(data.getlist('parent_version[]')[idx]),
            ]
            for (idx, hub_id) in enumerate(data.getlist('parent_hub_id[]'))
        ]

        try:
            execute_action(
                NewDatasetVersion(hub_id,
                                  dataset_id,
                                  data['backend'],
                                  data['path'],
                                  data.getlist('partition_key[]'),
                                  data['description'],
                                  bool(data.get('is_overlapping')),
                                  columns,
                                  depends_on)
            )
            return flask.redirect(flask.url_for('versions.versions_index_html', hub_id=hub_id, dataset_id=dataset_id))
        except DbException as e:
            error = str(e)

    published = fetch_view(PublishedVersions())
    return flask.render_template('versions/new.html.j2',
                                 hub_id=hub_id,
                                 dataset_id=dataset_id,
                                 backends=Backends,
                                 types=Types,
                                 error=error,
                                 published=published)


@bp.route('/<int:version>/detail.json', methods=['GET'])
def version_detail_json(hub_id, dataset_id, version):
    check_assertion(VersionExists(hub_id, dataset_id, version))
    return flask.jsonify(fetch_view(DetailVersion(hub_id, dataset_id, version)))


@bp.route('/<int:version>/detail.html', methods=['GET'])
def version_detail_html(hub_id, dataset_id, version):
    check_assertion(VersionExists(hub_id, dataset_id, version))
    details = fetch_view(DetailVersion(hub_id, dataset_id, version))
    return flask.render_template('versions/detail.html.j2',
                                 hub_id=hub_id,
                                 dataset_id=dataset_id,
                                 version_int=version,
                                 **details)


@bp.route('/<int:version>/publish.json', methods=['POST'])
def version_publish_json(hub_id, dataset_id, version):
    check_assertion(VersionExists(hub_id, dataset_id, version))
    execute_action(PublishVersion(hub_id, dataset_id, version))
    return flask.jsonify({})


@bp.route('/<int:version>/publish.html', methods=['POST'])
def version_publish_html(hub_id, dataset_id, version):
    check_assertion(VersionExists(hub_id, dataset_id, version))
    execute_action(PublishVersion(hub_id, dataset_id, version))
    return flask.redirect(flask.url_for('versions.versions_index_html', hub_id=hub_id, dataset_id=dataset_id))
