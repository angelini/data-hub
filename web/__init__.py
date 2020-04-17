import datetime as dt
import logging as std_logging
import time
import sys
import uuid

import flask
import flask_jwt_extended as flask_jwt
import jwt
import psycopg2 as psql
import psycopg2.pool
import structlog

from core.engine import logging
from web.db import AssertionFailure, DbException


def format_datetime(value):
    if value:
        return value.strftime('%B %d, %Y %H:%M:%S')
    return ''


def format_tooltip(partition):
    tooltip = f'pos:left; title:{partition["status"].capitalize()}'
    if partition['updated_at']:
        tooltip += f' - {format_datetime(partition["updated_at"])}'
    return tooltip


def create_app():
    logging.configure()
    werkzeug_log = std_logging.getLogger('werkzeug')
    werkzeug_log.setLevel(std_logging.ERROR)

    app = flask.Flask(__name__, instance_relative_config=True)

    # FIXME: Add production config and keys
    app.config['SECRET_KEY'] = 'dev'
    app.config['JWT_SECRET_KEY'] = 'dev'
    app.config['JWT_TOKEN_LOCATION'] = ('headers', 'cookies')
    app.config['JWT_COOKIE_SECURE'] = False
    app.config['JWT_COOKIE_CSRF_PROTECT'] = False
    app.config['JWT_ACCESS_TOKEN_EXPIRES'] = dt.timedelta(hours=12)

    app.config['db_pool'] = psql.pool.SimpleConnectionPool(1, 20, '')

    flask_jwt.JWTManager(app)

    from . import auth
    app.register_blueprint(auth.bp)

    from . import users
    app.register_blueprint(users.bp)

    from . import teams
    app.register_blueprint(teams.bp)

    from . import hubs
    app.register_blueprint(hubs.bp)

    from . import datasets
    app.register_blueprint(datasets.bp)

    from . import versions
    app.register_blueprint(versions.bp)

    from . import partitions
    app.register_blueprint(partitions.bp)

    from . import connections
    app.register_blueprint(connections.bp)

    app.jinja_env.filters['datetime'] = format_datetime
    app.jinja_env.filters['tooltip'] = format_tooltip

    @app.before_request
    def before_request_logger():
        flask.g.start_time = time.time()
        logging.init(request_id=uuid.uuid4())
        logging.info('start_request',
                     method=flask.request.method,
                     host=flask.request.host_url,
                     path=flask.request.path,
                     endpoint=getattr(flask.request.url_rule, 'endpoint', ''))

    exclude_authorization = [
        'auth.login_html', 'auth.login_json', 'redirect_index', 'static', 'users.new_json',
    ]

    @app.before_request
    def before_request_authorization():
        endpoint = getattr(flask.request.url_rule, 'endpoint', '')
        if flask.request.path == '/favicon.ico' or endpoint in exclude_authorization:
            return

        try:
            flask_jwt.verify_jwt_in_request()
        except (flask_jwt.exceptions.JWTExtendedException, jwt.ExpiredSignature):
            if flask.request.url.endswith('html'):
                return flask.redirect(flask.url_for('auth.login_html'))
            return flask.jsonify({'error': 'missing access token'}), 401

    @app.after_request
    def after_request_logger(response):
        log = structlog.get_logger()
        log.info('stop_request',
                 code=response.status_code,
                 time=round((time.time() - flask.g.start_time) * 1000, ndigits=4),
                 method=flask.request.method,
                 host=flask.request.host_url,
                 path=flask.request.path,
                 endpoint=getattr(flask.request.url_rule, 'endpoint', ''))
        return response

    @app.teardown_appcontext
    def close_db_pool(e):
        db = flask.g.pop('db', None)
        if db is not None:
            app.config['db_pool'].putconn(db)

    @app.errorhandler(DbException)
    def handle_db_exception(error):
        if flask.request.url.endswith('html'):
            return flask.render_template('error.html.j2', error=str(error)), 500
        return flask.jsonify({'error': str(error)}), 500

    @app.errorhandler(AssertionFailure)
    def handle_assertion_failure(error):
        if flask.request.url.endswith('html'):
            return flask.render_template('error.html.j2', error=str(error)), error.status_code
        return flask.jsonify({'error': str(error)}), error.status_code

    @app.route('/', methods=['GET'])
    def redirect_index():
        return flask.redirect(flask.url_for('hubs.index_html'))

    return app
