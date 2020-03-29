import psycopg2 as psql
import psycopg2.extras

psql.extras.register_uuid()


class DbException(Exception):
    pass


class AssertionFailure(Exception):

    def __init__(self, message, status_code):
        super().__init__(message)
        self.status_code = status_code


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
    except psql.DatabaseError as e:
        raise DbException(e.diag.message_primary)
    finally:
        conn.close()


def check_assertion(assertion):
    conn = connect()
    try:
        cursor = conn.cursor()
        if not assertion.check(cursor):
            raise AssertionFailure(assertion.message(), assertion.status_code)
    finally:
        conn.close()
