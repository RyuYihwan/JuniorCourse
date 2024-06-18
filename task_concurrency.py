import config
from enum import Enum

import psycopg2

import logging
import traceback


class Status(Enum):
    PENDING = "PN"
    RUNNING = "RN"
    SUCCESS = "SC"
    FAILURE = "FL"


DROP_TABLE_QUERY = "DROP TABLE IF EXISTS test.work;"
CREATE_TABLE_QUERY = "CREATE TABLE IF NOT EXISTS test.work (id SERIAL PRIMARY KEY, STATUS VARCHAR(32), JOB_NAME VARCHAR(32));"
INITIALIZE_DATA_QUERY = "INSERT INTO test.work (status, job_name) VALUES (%s, %s);"
STATUS_CHANGE_QUERY = "UPDATE test.work SET status=%s WHERE id=%s AND status=%s;"


def set_log():
    logging.basicConfig(filename='database_errors.log', level=logging.ERROR,
                        format='%(asctime)s %(levelname)s:%(message)s')


def get_cursor_by_db_connect():
    try:
        db = psycopg2.connect(host='localhost', dbname='postgres', user=config.user, password=config.password,
                              port=5432)

        db.set_client_encoding('UTF8')

        db_cursor = db.cursor()

    except:
        print(traceback.format_exc())
        logging.error(traceback.format_exc())
    else:
        return db_cursor


def use_cursor_by_db_connect(query, params=None):
    db = ''
    try:
        db = psycopg2.connect(host='localhost', dbname='postgres', user=config.user, password=config.password,
                              port=5432)

        db.set_client_encoding('UTF8')

        db_cursor = db.cursor()
        if params:
            cursor.execute(query, params)
        else:
            cursor.execute(query)

        db_cursor.commit()

    except:
        print(traceback.format_exc())
        logging.error(traceback.format_exc())
    finally:
        if db:
            db.close()


if __name__ == '__main__':
    set_log()

    # use_cursor_by_db_connect(DROP_TABLE_QUERY)
    #
    # use_cursor_by_db_connect(CREATE_TABLE_QUERY)
    #
    # for i in range(1, 21):
    #     use_cursor_by_db_connect(INITIALIZE_DATA_QUERY, (Status.PENDING.value, '배치 JOB' + str(i)))
    #
    # for i in range(1, 21):
    #     use_cursor_by_db_connect(STATUS_CHANGE_QUERY, (Status.RUNNING.value, i, Status.PENDING.value))

    cursor = get_cursor_by_db_connect()
    cursor.execute('SELECT * FROM test.work;')
    data = cursor.fetchall()
    print(len(data))
    first_status = data[0][1]
    print(first_status)


