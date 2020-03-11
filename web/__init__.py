from flask import Flask


def format_datetime(value):
    # return value.strftime('%Y-%m-%d %H:%M:%S %Z')
    return value.strftime('%B %d, %Y %H:%M:%S')


def create_app():
    app = Flask(__name__, instance_relative_config=True)
    app.config.from_mapping(SECRET_KEY='dev')

    from . import hubs
    app.register_blueprint(hubs.bp)

    from . import datasets
    app.register_blueprint(datasets.bp)

    from . import versions
    app.register_blueprint(versions.bp)

    from . import partitions
    app.register_blueprint(partitions.bp)

    app.jinja_env.filters['datetime'] = format_datetime

    return app
