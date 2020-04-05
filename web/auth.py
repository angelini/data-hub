from functools import wraps

import flask
import flask_jwt_extended as flask_jwt

from core.data import AccessLevel
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


def is_current_hub_reader():
    hub_id = str(flask.request.view_args['hub_id'])
    roles = flask_jwt.get_jwt_claims()
    return AccessLevel.can_read(roles.get(str(hub_id), 'none'))


def is_current_hub_writer():
    hub_id = str(flask.request.view_args['hub_id'])
    roles = flask_jwt.get_jwt_claims()
    return AccessLevel.can_write(roles.get(str(hub_id), 'none'))


def auth_current_hub_reader():
    hub_id = str(flask.request.view_args['hub_id'])

    if not is_current_hub_reader():
        error = f'unauthorized access to {hub_id}'
        if flask.request.url.endswith('html'):
            return flask.render_template('error.html.j2', error=error), 401
        return flask.jsonify({'error': error}), 401


def auth_current_hub_writer():
    hub_id = str(flask.request.view_args['hub_id'])

    if not is_current_hub_writer():
        error = f'unauthorized access to {hub_id}'
        if flask.request.url.endswith('html'):
            return flask.render_template('error.html.j2', error=error), 401
        return flask.jsonify({'error': error}), 401


def require_writer(fn):
    @wraps(fn)
    def check_access(*args, **kwargs):
        error = auth_current_hub_writer()
        if error:
            return error
        return fn(*args, **kwargs)

    return check_access
