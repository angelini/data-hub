import flask

from core.engine import DetailTeam, ListTeams, NewTeam
from web.db import DbException, fetch_view, execute_action

bp = flask.Blueprint('teams', __name__, url_prefix='/teams')


@bp.route('/index.json', methods=['GET'])
def teams_index_json():
    return flask.jsonify(fetch_view(ListTeams()))


@bp.route('/index.html', methods=['GET'])
def teams_index_html():
    return flask.render_template('teams/index.html.j2', **fetch_view(ListTeams()))


@bp.route('/new.json', methods=['POST'])
def team_new_json():
    data = flask.request.json
    team_id = execute_action(NewTeam(data['name']))
    return flask.jsonify({'team_id': team_id}), 201


@bp.route('/new.html', methods=['GET', 'POST'])
def team_new_html():
    error = None

    if flask.request.method == 'POST':
        data = flask.request.form
        try:
            execute_action(NewTeam(data['name']))
            return flask.redirect(flask.url_for('teams.teams_index_html'))
        except DbException as e:
            error = str(e)

    return flask.render_template('teams/new.html.j2', error=error)


@bp.route('/<uuid:team_id>/detail.json', methods=['GET'])
def team_detail_json(team_id):
    return flask.jsonify(fetch_view(DetailTeam(team_id)))


@bp.route('/<uuid:team_id>/detail.html', methods=['GET'])
def team_detail_html(team_id):
    details = fetch_view(DetailTeam(team_id))
    return flask.render_template('teams/detail.html.j2',
                                 team_id=team_id,
                                 **details)
