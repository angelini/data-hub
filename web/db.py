import functools

import flask
import pq
import psycopg2 as psql
import psycopg2.extras

from core.job import Job

psql.extras.register_uuid()


class DbException(Exception):
    pass


def raise_as_dbexception(fn):
    @functools.wraps(fn)
    def handler(*args, **kwargs):
        try:
            return fn(*args, **kwargs)
        except psql.DatabaseError as e:
            raise DbException(e.diag.message_primary)

    return handler


class AssertionFailure(Exception):

    def __init__(self, message, status_code):
        super().__init__(message)
        self.status_code = status_code


def connect():
    if 'db' not in flask.g:
        flask.g.db = flask.current_app.config['db_pool'].getconn()
        flask.g.queue = pq.PQ(conn=flask.g.db)['jobs']
    return flask.g.db


@raise_as_dbexception
def enqueue_job(backend_id, action, config):
    connect()
    return flask.g.queue.put(Job(backend_id, action, config).__dict__)


@raise_as_dbexception
def fetch_view(view):
    conn = connect()
    cursor = conn.cursor()
    return view.fetch(cursor)


@raise_as_dbexception
def execute_action(action):
    conn = connect()
    cursor = conn.cursor()
    result = action.execute(cursor)
    conn.commit()
    return result


@raise_as_dbexception
def check_assertion(assertion):
    conn = connect()
    cursor = conn.cursor()
    if not assertion.check(cursor):
        raise AssertionFailure(assertion.message(), assertion.status_code)
