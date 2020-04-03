import flask

from core.engine import DetailTeam, ListHubs, ListTeams, ListUsers, NewTeam, NewTeamMember, TeamExists
from web.db import check_assertion, DbException, fetch_view, execute_action

bp = flask.Blueprint('teams', __name__, url_prefix='/teams')


@bp.route('/index.json', methods=['GET'])
def index_json():
    return flask.jsonify(fetch_view(ListTeams()))


@bp.route('/index.html', methods=['GET'])
def index_html():
    return flask.render_template('teams/index.html.j2', **fetch_view(ListTeams()))


@bp.route('/new.json', methods=['POST'])
def new_json():
    data = flask.request.json
    team_id = execute_action(NewTeam(data['name']))
    return flask.jsonify({'team_id': team_id}), 201


@bp.route('/new.html', methods=['GET', 'POST'])
def new_html():
    error = None

    if flask.request.method == 'POST':
        data = flask.request.form
        try:
            execute_action(NewTeam(data['name']))
            return flask.redirect(flask.url_for('teams.index_html'))
        except DbException as e:
            error = str(e)

    return flask.render_template('teams/new.html.j2', error=error)


@bp.route('/<uuid:team_id>/detail.json', methods=['GET'])
def detail_json(team_id):
    check_assertion(TeamExists(team_id))
    return flask.jsonify(fetch_view(DetailTeam(team_id)))


@bp.route('/<uuid:team_id>/detail.html', methods=['GET'])
def detail_html(team_id):
    check_assertion(TeamExists(team_id))
    details = fetch_view(DetailTeam(team_id))
    return flask.render_template('teams/detail.html.j2',
                                 team_id=team_id,
                                 **details)


@bp.route('/<uuid:team_id>/new_member.json', methods=['POST'])
def new_member_json(team_id):
    data = flask.request.json
    check_assertion(TeamExists(team_id))
    member_id = execute_action(NewTeamMember(team_id, data['user_id']))
    return flask.jsonify({'member_id': member_id})


@bp.route('/<uuid:team_id>/new_member.html', methods=['POST'])
def new_member_html(team_id):
    data = flask.request.form
    check_assertion(TeamExists(team_id))
    execute_action(NewTeamMember(team_id, data['user_id']))
    return flask.redirect(flask.url_for('teams.detail_html', team_id=team_id))