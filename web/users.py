import flask

from core.engine import NewUser
from web.db import execute_action

bp = flask.Blueprint('users', __name__, url_prefix='/users')


@bp.route('/new.json', methods=['POST'])
def new_json():
    data = flask.request.json
    user_id = execute_action(NewUser(data['email'], data['password']))
    return flask.jsonify({'user_id': user_id}), 201
