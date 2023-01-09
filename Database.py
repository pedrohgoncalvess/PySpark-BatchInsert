import psycopg2
from psycopg2.extensions import connection
from __init__ import environment_variables
def connect() -> connection:

    conn = psycopg2.connect(
        host="localhost",
        database="postgres",
        user=environment_variables('USER_DB'),
        password=environment_variables('PASSWORD_DB'))
    conn.autocommit = True
    return conn

