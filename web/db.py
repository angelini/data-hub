import flask
import pq
import psycopg2 as psql
import psycopg2.extras

from core.job import Job

psql.extras.register_uuid()


class DbException(Exception):
    pass


class AssertionFailure(Exception):

    def __init__(self, message, status_code):
        super().__init__(message)
        self.status_code = status_code


def connect():
    if 'db' not in flask.g:
        flask.g.db = flask.current_app.config['db_pool'].getconn()
        flask.g.queue = pq.PQ(conn=flask.g.db)['jobs']
    return flask.g.db


def enqueue_job(backend_id, action, config):
    connect()
    return flask.g.queue.put(Job(backend_id, action, config).__dict__)


def fetch_view(view):
    conn = connect()
    cursor = conn.cursor()
    return view.fetch(cursor)


def execute_action(action):
    conn = connect()
    try:
        cursor = conn.cursor()
        result = action.execute(cursor)
        conn.commit()
        return result
    except psql.DatabaseError as e:
        raise DbException(e.diag.message_primary)


def check_assertion(assertion):
    conn = connect()
    cursor = conn.cursor()
    if not assertion.check(cursor):
        raise AssertionFailure(assertion.message(), assertion.status_code)
