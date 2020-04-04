import flask
import flask_jwt_extended as flask_jwt

from core.engine import CorrectPassword, DetailUser
from web.db import AssertionFailure, check_assertion, fetch_view

bp = flask.Blueprint('auth', __name__, url_prefix='/auth')


@bp.route('/login.json', methods=['POST'])
def login_json():
    data = flask.request.json
    email = data.get('email')
    password = data.get('password')

    if not email or not password:
        return flask.jsonify({}), 401

    check_assertion(CorrectPassword(email, password))
    details = fetch_view(DetailUser(email))
    return flask.jsonify({
        'access_token': flask_jwt.create_access_token(details['user_id'],
                                                      user_claims=details['roles'])
    })


@bp.route('/login.html', methods=['GET', 'POST'])
def login_html():
    error = None

    if flask.request.method == 'POST':
        data = flask.request.form
        try:
            check_assertion(CorrectPassword(data['email'], data['password']))

            details = fetch_view(DetailUser(data['email']))
            access_token = flask_jwt.create_access_token(details['user_id'],
                                                         user_claims=details['roles'])

            response = flask.redirect(flask.url_for('hubs.index_html'))
            flask_jwt.set_access_cookies(response, access_token)

            return response

        except AssertionFailure as e:
            error = str(e)

    return flask.render_template('auth/login.html.j2', error=error)