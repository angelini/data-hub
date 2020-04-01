import logging
import time
import sys
import uuid

import flask
import flask_jwt_extended as flask_jwt
import structlog

from web.db import AssertionFailure, DbException


def format_datetime(value):
    if value:
        # return value.strftime('%Y-%m-%d %H:%M:%S %Z')
        return value.strftime('%B %d, %Y %H:%M:%S')
    return ''


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
            structlog.processors.TimeStamper(fmt="%d-%m-%Y %H:%M:%S"),
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
    app.config['JWT_COOKIE_CSRF_PROTECT'] = True

    flask_jwt.JWTManager(app)

    from . import auth
    app.register_blueprint(auth.bp)

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

    app.jinja_env.filters['datetime'] = format_datetime

    @app.before_request
    def before_request_func():
        flask.g.start_time = time.time()
        log = logger.new(request_id=str(uuid.uuid4()))
        log.info('request_start', method=flask.request.method, url=flask.request.url)

    @app.after_request
    def after_request_func(response):
        log = structlog.get_logger()
        log.info('request_stop', code=response.status_code, time=(time.time() - flask.g.start_time))
        return response

    @app.errorhandler(DbException)
    def handle_db_exception(error):
        if flask.request.url.endswith('html'):
            return flask.render_template('error.html.j2', error=str(error)), 500
        response = flask.jsonify({'error': str(error)})
        response.status_code = 500
        return response

    @app.errorhandler(AssertionFailure)
    def handle_assertion_failure(error):
        if flask.request.url.endswith('html'):
            return flask.render_template('error.html.j2', error=str(error)), error.status_code
        response = flask.jsonify({'error': str(error)})
        response.status_code = error.status_code
        return response

    @app.route('/', methods=['GET'])
    def redirect_index():
        return flask.redirect(flask.url_for('hubs.hubs_index_html'))

    return app
