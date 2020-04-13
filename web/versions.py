import flask

from core.data import Backends, Types
from core.engine import DatasetExists, DetailDataset, DetailVersion, ListVersions, NewDatasetVersion, \
    PublishedVersions, PublishVersion, SetQueuedPartitionStatus, SimpleDetailVersion, VersionExists
from web.auth import auth_current_hub_reader, is_current_hub_writer, require_writer
from web.db import DbException, check_assertion, fetch_view, enqueue_job, execute_action

bp = flask.Blueprint('versions', __name__, url_prefix='/hubs/<uuid:hub_id>/datasets/<uuid:dataset_id>/versions')


@bp.before_request
def authorize_before_request():
    return auth_current_hub_reader()


@bp.route('/index.json', methods=['GET'])
def index_json(hub_id, dataset_id):
    return flask.jsonify(fetch_view(ListVersions(hub_id, dataset_id)))


@bp.route('/index.html', methods=['GET'])
def index_html(hub_id, dataset_id):
    check_assertion(DatasetExists(dataset_id))
    details = fetch_view(DetailDataset(hub_id, dataset_id))
    versions = fetch_view(ListVersions(hub_id, dataset_id))
    return flask.render_template('versions/index.html.j2',
                                 hub_id=hub_id,
                                 dataset_id=dataset_id,
                                 is_writer=is_current_hub_writer(),
                                 **details,
                                 **versions)


@bp.route('/new.json', methods=['POST'])
@require_writer
def new_json(hub_id, dataset_id):
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
@require_writer
def new_html(hub_id, dataset_id):
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
            return flask.redirect(flask.url_for('versions.index_html', hub_id=hub_id, dataset_id=dataset_id))
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
def detail_json(hub_id, dataset_id, version):
    check_assertion(VersionExists(hub_id, dataset_id, version))
    return flask.jsonify(fetch_view(DetailVersion(hub_id, dataset_id, version)))


@bp.route('/<int:version>/detail.html', methods=['GET'])
def detail_html(hub_id, dataset_id, version):
    check_assertion(VersionExists(hub_id, dataset_id, version))
    dataset_details = fetch_view(DetailDataset(hub_id, dataset_id))
    details = fetch_view(DetailVersion(hub_id, dataset_id, version))
    return flask.render_template('versions/detail.html.j2',
                                 hub_id=hub_id,
                                 dataset_id=dataset_id,
                                 version_int=version,
                                 is_writer=is_current_hub_writer(),
                                 **dataset_details,
                                 **details)


@bp.route('/<int:version>/dependencies.html', methods=['GET'])
def dependencies_html(hub_id, dataset_id, version):
    check_assertion(VersionExists(hub_id, dataset_id, version))
    details = fetch_view(DetailVersion(hub_id, dataset_id, version))
    return flask.render_template('versions/dependencies.html.j2',
                                 hub_id=hub_id,
                                 dataset_id=dataset_id,
                                 version_int=version,
                                 is_writer=is_current_hub_writer(),
                                 **details)


@bp.route('/<int:version>/publish.json', methods=['POST'])
@require_writer
def publish_json(hub_id, dataset_id, version):
    check_assertion(VersionExists(hub_id, dataset_id, version))
    execute_action(PublishVersion(hub_id, dataset_id, version))
    return flask.jsonify({})


@bp.route('/<int:version>/publish.html', methods=['POST'])
@require_writer
def publish_html(hub_id, dataset_id, version):
    check_assertion(VersionExists(hub_id, dataset_id, version))
    execute_action(PublishVersion(hub_id, dataset_id, version))
    return flask.redirect(flask.url_for('versions.index_html', hub_id=hub_id, dataset_id=dataset_id))


@bp.route('/<int:version>/verify.json', methods=['POST'])
def verify_json(hub_id, dataset_id, version):
    check_assertion(VersionExists(hub_id, dataset_id, version))
    execute_action(SetQueuedPartitionStatus(hub_id, dataset_id, version))
    backend_id = fetch_view(SimpleDetailVersion(hub_id, dataset_id, version))['backend_id']
    queue_id = enqueue_job(backend_id, 'verify_partitions', {
        'hub_id': str(hub_id),
        'dataset_id': str(dataset_id),
        'version': str(version)
    })
    return flask.jsonify({'queue_id': queue_id})


@bp.route('/<int:version>/verify.html', methods=['POST'])
def verify_html(hub_id, dataset_id, version):
    check_assertion(VersionExists(hub_id, dataset_id, version))
    execute_action(SetQueuedPartitionStatus(hub_id, dataset_id, version))
    backend_id = fetch_view(SimpleDetailVersion(hub_id, dataset_id, version))['backend_id']
    enqueue_job(backend_id, 'verify_partitions', {
        'hub_id': str(hub_id),
        'dataset_id': str(dataset_id),
        'version': str(version)
    })
    return flask.redirect(flask.url_for('versions.detail_html', hub_id=hub_id, dataset_id=dataset_id, version=version))
