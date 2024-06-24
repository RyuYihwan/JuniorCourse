from datetime import datetime
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
SELECT_BATCH_JOB_QUERY = "SELECT * FROM test.work WHERE id=%s AND status=%s FOR UPDATE"

CREATE_LOG_TABLE_QUERY = "CREATE TABLE IF NOT EXISTS test.tlog (log_id SERIAL PRIMARY KEY, job_id INT, content VARCHAR(32), execution_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP);"
DELETE_LOG_QUERY = "DELETE FROM test.tlog"
DROP_LOG_TABLE_QUERY = "DROP TABLE IF EXISTS test.tlog;"

INSERT_LOG_QUERY = "INSERT INTO test.tlog (job_id, content) VALUES (%s, %s);"


def set_log():
    logging.basicConfig(filename='database_errors.log', level=logging.ERROR,
                        format='%(asctime)s %(levelname)s:%(message)s')


def get_db_connection_pool():
    try:
        db_conn_pool = psycopg2.pool.ThreadedConnectionPool(
            400, 800,
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
        cursor = db_conn.cursor()
        cursor.execute(CREATE_TABLE_QUERY)
        cursor.execute(DELETE_DATA_QUERY)
        cursor.execute(DROP_LOG_TABLE_QUERY)
        cursor.execute(CREATE_LOG_TABLE_QUERY)

        for i in range(1, 21):
            cursor.execute(INSERT_DATA_QUERY, (i, Status.PENDING.value, '배치 JOB' + str(i)))

        db_conn.commit()

    except:
        logging.error(traceback.format_exc())
        print(traceback.format_exc())


def change_status_job(db_conn, t_id, former_status, later_status):
    cursor = db_conn.cursor()
    cursor.execute(STATUS_CHANGE_QUERY, (later_status, t_id, former_status))


def change_status_job_no_check(db_conn, t_id, later_status):
    cursor = db_conn.cursor()
    cursor.execute(STATUS_CHANGE_NO_CHECK_QUERY, (later_status, t_id))


def batch_worker(db_conn_pool, b_id, t_id, msg):
    global job
    db_conn = db_conn_pool.getconn()

    # 수동 트랜젝션 관리
    db_conn.autocommit = False

    # 커서 획득
    cursor = db_conn.cursor()

    try:
        print(f'{msg}가 작업을 시작했습니다.')
        cursor.execute(SELECT_BATCH_JOB_QUERY, (b_id, Status.PENDING.value))

        job = cursor.fetchone()

        if job:
            # 상태 변경 Pending -> Running
            change_status_job(db_conn, b_id, Status.PENDING.value, Status.RUNNING.value)

            time.sleep(5)

            # 상태 변경 Running -> Success
            change_status_job(db_conn, b_id, Status.RUNNING.value, Status.SUCCESS.value)

            time.sleep(5)

            # 성공 로그 남김
            cursor.execute(INSERT_LOG_QUERY, (b_id, f'{Status.SUCCESS.value}으로 완료 되었습니다.'))

            time.sleep(5)

            # 성공 로그 기록 후 다른 스레드 접근을 위해 상태 변경 Success -> Pending
            change_status_job(db_conn, b_id, Status.SUCCESS.value, Status.PENDING.value)

            time.sleep(5)

            print(f'{msg}가 작업을 완료했습니다.')
        else:
            raise Exception(f'현재 batch job을 찾을 수 없습니다. - {msg}')

    except:
        print(f'{msg}의 작업이 실패 했습니다.')
        print(traceback.format_exc())
        logging.error(traceback.format_exc())
        change_status_job_no_check(db_conn, t_id, Status.FAILURE.value)

        cursor.execute(INSERT_LOG_QUERY, (b_id, f'{msg} failed'))

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

    prev_time = datetime.now()

    # 각 batch job 마다 20개씩 스레드를 만들어서 실행.
    # 이후 batch job 실행이 전부 성공 했는지 확인.
    for i in range(1, 21):
        for j in range(1, 21):
            message = f'batch job {i}의 {j}번 스레드'
            t = threading.Thread(target=batch_worker, name=f'Thread-{i}-{j}', args=(conn_pool, i, j, message))
            threads.append(t)
            t.start()

    for t in threads:
        t.join()

    conn_pool.closeall()

    diff = datetime.now() - prev_time

    print('{} 분 소요: '.format(round(diff.seconds / 60, 2)))
