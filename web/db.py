import psycopg2 as psql
import psycopg2.extras

psql.extras.register_uuid()


class DbException(Exception):
    pass


def connect():
    return psql.connect('dbname=dh user=postgres host=localhost')


def fetch_view(view):
    conn = connect()
    try:
        cursor = conn.cursor()
        return view.fetch(cursor)
    finally:
        conn.close()


def execute_action(action):
    conn = connect()
    try:
        cursor = conn.cursor()
        result = action.execute(cursor)
        conn.commit()
        return result
    except psql.errors.DatabaseError as e:
        raise DbException(e.diag.message_primary)
    finally:
        conn.close()


def check_assertion(assertion):
    conn = connect()
    try:
        cursor = conn.cursor()
        return assertion.check(cursor)
    finally:
        conn.close()
