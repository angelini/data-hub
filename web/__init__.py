import datetime as dt
import logging
import time
import sys
import uuid

import flask
import flask_jwt_extended as flask_jwt
import jwt
import psycopg2 as psql
import psycopg2.pool
import structlog

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
    logging.basicConfig(format='%(message)s', stream=sys.stdout, level=logging.INFO)
    log = logging.getLogger('werkzeug')
    log.setLevel(logging.ERROR)

    structlog.configure(
        processors=[
            structlog.stdlib.filter_by_level,
            structlog.processors.StackInfoRenderer(),
            structlog.dev.set_exc_info,
            structlog.processors.format_exc_info,
            structlog.processors.TimeStamper(fmt='%d-%m-%Y %H:%M:%S'),
            structlog.dev.ConsoleRenderer(),
        ],
        context_class=structlog.threadlocal.wrap_dict(dict),
        logger_factory=structlog.stdlib.LoggerFactory(),
    )

    logger = structlog.get_logger()

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
        log = logger.new(request_id=str(uuid.uuid4()))
        log.info('request_start', method=flask.request.method, url=flask.request.url)

    exclude_authorization = [
        'auth.login_html', 'auth.login_json', 'redirect_index', 'static', 'users.new_json',
    ]

    @app.before_request
    def before_request_authorization():
        rule = flask.request.url_rule
        if flask.request.path == '/favicon.ico' or (rule and rule.endpoint in exclude_authorization):
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
        log.info('request_stop', code=response.status_code, time=(time.time() - flask.g.start_time))
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
