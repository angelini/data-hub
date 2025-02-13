import flask
import flask_jwt_extended as flask_jwt

from core.data import AccessLevel
from core.engine.actions import NewHub
from core.engine.views import ListHubs, ListTeams
from web.db import DbException, fetch_view, execute_action

bp = flask.Blueprint('hubs', __name__, url_prefix='/hubs')


def list_readable_hubs():
    roles = flask_jwt.get_jwt_claims()
    return {
        'hubs': [
            hub
            for hub in fetch_view(ListHubs())['hubs']
            if AccessLevel.can_read(roles.get(str(hub['id']), 'none'))
        ]
    }


@bp.route('/index.json', methods=['GET'])
def index_json():
    return flask.jsonify(list_readable_hubs())


@bp.route('/index.html', methods=['GET'])
def index_html():
    return flask.render_template('hubs/index.html.j2', **list_readable_hubs())


@bp.route('/new.json', methods=['POST'])
def new_json():
    data = flask.request.json
    hub_id = execute_action(NewHub(data['team_id'], data['name']))
    return flask.jsonify({'hub_id': hub_id}), 201


@bp.route('/new.html', methods=['GET', 'POST'])
def new_html():
    error = None

    if flask.request.method == 'POST':
        data = flask.request.form
        try:
            execute_action(NewHub(data['team_id'], data['name']))
            return flask.redirect(flask.url_for('hubs.index_html'))
        except DbException as e:
            error = str(e)

    teams = fetch_view(ListTeams())['teams']
    return flask.render_template('hubs/new.html.j2', teams=teams, error=error)
