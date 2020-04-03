import flask
import passlib.context

bp = flask.Blueprint('auth', __name__, url_prefix='/auth')


@bp.route('/login.json', methods=['POST'])
def login_json():
    data = flask.request.json
    email = data.get('email')
    password = data.get('password')

    if not email or not password:
        return flask.jsonify({}), 401
