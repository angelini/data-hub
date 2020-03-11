import flask

from core.engine import NewPartition
from web.db import write_action

bp = flask.Blueprint('partitions', __name__,
                     url_prefix='/hubs/<uuid:hub_id>/datasets/<uuid:dataset_id>/versions/<int:version>')


@bp.route('/new.json', methods=['POST'])
def partition_new_json(hub_id, dataset_id, version):
    data = flask.json()
    partition_id = write_action(
        NewPartition(hub_id,
                     dataset_id,
                     version,
                     data['values'],
                     data['path'],
                     data.get('row_count'),
                     data.get('start_time'),
                     data.get('end_time'))
    )
    return flask.jsonify({'partition_id': partition_id})


@bp.route('/new.html', methods=['GET', 'POST'])
def partition_new_html(hub_id, dataset_id, version):
    if flask.request.method == 'POST':
        data = flask.request.form
        write_action(
            NewPartition(hub_id,
                         dataset_id,
                         version,
                         data.getlist('values[]'),
                         data['path'],
                         data.get('row_count'),
                         data.get('start_time'),
                         data.get('end_time'))
        )
        return flask.redirect(flask.url_for('versions.version_detail_html',
                                            hub_id=hub_id, dataset_id=dataset_id, version=version))

    return flask.render_template('partitions/new.html.j2',
                                 hub_id=hub_id,
                                 dataset_id=dataset_id,
                                 version=version)
