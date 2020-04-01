import flask

from core.engine import ListHubs, ListTeams, NewHub
from web.db import DbException, fetch_view, execute_action

bp = flask.Blueprint('hubs', __name__, url_prefix='/hubs')


@bp.route('/index.json', methods=['GET'])
def hubs_index_json():
    return flask.jsonify(fetch_view(ListHubs()))


@bp.route('/index.html', methods=['GET'])
def hubs_index_html():
    return flask.render_template('hubs/index.html.j2', **fetch_view(ListHubs()))


@bp.route('/new.json', methods=['POST'])
def hub_new_json():
    data = flask.request.json
    hub_id = execute_action(NewHub(data['team_id'], data['name']))
    return flask.jsonify({'hub_id': hub_id}), 201


@bp.route('/new.html', methods=['GET', 'POST'])
def hub_new_html():
    error = None

    if flask.request.method == 'POST':
        data = flask.request.form
        try:
            execute_action(NewHub(data['team_id'], data['name']))
            return flask.redirect(flask.url_for('hubs.hubs_index_html'))
        except DbException as e:
            error = str(e)

    teams = fetch_view(ListTeams())['teams']
    return flask.render_template('hubs/new.html.j2', teams=teams, error=error)
