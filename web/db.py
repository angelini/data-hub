import psycopg2 as psql
import psycopg2.extras

psql.extras.register_uuid()


class DbException(Exception):
    pass


def read_view(view):
    conn = psql.connect('dbname=dh user=postgres host=localhost')
    try:
        cursor = conn.cursor()
        return view.fetch(cursor)
    finally:
        conn.close()


def write_action(action):
    conn = psql.connect('dbname=dh user=postgres host=localhost')
    try:
        cursor = conn.cursor()
        result = action.execute(cursor)
        conn.commit()
        return result
    except psql.errors.DatabaseError as e:
        raise DbException(e.diag.message_primary)
    finally:
        conn.close()
