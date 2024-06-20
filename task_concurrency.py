import logging
import threading
import time
import traceback
from enum import Enum

import psycopg2.pool

import config


class Status(Enum):
    PENDING = "PN"
    RUNNING = "RN"
    SUCCESS = "SC"
    FAILURE = "FL"


DROP_TABLE_QUERY = "DROP TABLE IF EXISTS test.work;"
DELETE_DATA_QUERY = "DELETE FROM test.work"
CREATE_TABLE_QUERY = "CREATE TABLE IF NOT EXISTS test.work (id SERIAL PRIMARY KEY, STATUS VARCHAR(32), JOB_NAME VARCHAR(32));"
INSERT_DATA_QUERY = "INSERT INTO test.work (id, status, job_name) VALUES (%s, %s, %s);"
STATUS_CHANGE_QUERY = "UPDATE test.work SET status=%s WHERE id=%s AND status=%s;"
STATUS_CHANGE_NO_CHECK_QUERY = "UPDATE test.work SET status=%s WHERE id=%s;"


def set_log():
    logging.basicConfig(filename='database_errors.log', level=logging.ERROR,
                        format='%(asctime)s %(levelname)s:%(message)s')


def get_db_connection_pool():
    try:
        db_conn_pool = psycopg2.pool.ThreadedConnectionPool(
            50, 100,
            host='localhost',
            dbname='postgres',
            user=config.user,
            password=config.password,
            port=5432
        )

    except:
        print(traceback.format_exc())
        logging.error(traceback.format_exc())
    else:
        return db_conn_pool


def initialize_data(db_conn):
    try:
        db_conn.cursor().execute(CREATE_TABLE_QUERY)

        db_conn.cursor().execute(DELETE_DATA_QUERY)

        for i in range(1, 21):
            db_conn.cursor().execute(INSERT_DATA_QUERY, (i, Status.PENDING.value, '배치 JOB' + str(i)))

        db_conn.commit()

    except:
        logging.error(traceback.format_exc())
        print(traceback.format_exc())


def change_status_job(db_conn, t_id, former_status, later_status):
    db_conn.cursor().execute(STATUS_CHANGE_QUERY, (later_status, t_id, former_status))

    # db_conn.commit()


def change_status_job_no_check(db_conn, t_id, later_status):
    db_conn.cursor().execute(STATUS_CHANGE_NO_CHECK_QUERY, (later_status, t_id))

    # db_conn.commit()


def batch_worker(db_conn_pool, t_id, msg):
    db_conn = db_conn_pool.getconn()

    try:
        print(f'{msg}가 작업을 시작했습니다.')
        change_status_job(db_conn, t_id, Status.PENDING.value, Status.RUNNING.value)

        time.sleep(100)

        change_status_job(db_conn, t_id, Status.RUNNING.value, Status.SUCCESS.value)
        print(f'{msg}가 작업을 완료했습니다.')
    except:
        print(traceback.format_exc())
        logging.error(traceback.format_exc())
        change_status_job_no_check(db_conn, t_id, Status.FAILURE.value)

    finally:
        db_conn.commit()

        db_conn_pool.putconn(db_conn)


if __name__ == '__main__':
    set_log()

    conn_pool = get_db_connection_pool()

    conn = conn_pool.getconn()

    initialize_data(conn)

    conn_pool.putconn(conn)

    threads = []

    for j in range(3):
        for i in range(1, 21):
            message = f'{i}번 스레드'
            t = threading.Thread(target=batch_worker, name=f'Thread-{i}', args=(conn_pool, i, message))
            threads.append(t)
            t.start()

    for t in threads:
        t.join()

    conn_pool.closeall()
