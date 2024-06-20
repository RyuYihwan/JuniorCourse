import threading
import time
from enum import Enum

import psycopg2.pool

import config


class Status(Enum):
    PENDING = "PN"
    RUNNING = "RN"
    SUCCESS = "SC"
    FAILURE = "FL"


conn_pool = psycopg2.pool.ThreadedConnectionPool(
    1, 20,
    host='localhost',
    dbname='postgres',
    user=config.user,
    password=config.password,
    port=5432
)


def worker(t_id):
    conn = conn_pool.getconn()
    try:
        cursor = conn.cursor()
        cursor.execute("UPDATE test.work SET status = %s WHERE id = %s AND status = %s;",
                       (Status.RUNNING.value, t_id, Status.PENDING.value))
        time.sleep(0.1)  # Simulate some work
        cursor.execute("UPDATE test.work SET status = %s WHERE id = %s AND status = %s;",
                       (Status.SUCCESS.value, t_id, Status.RUNNING.value))
        conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"Error: {e}")
    finally:
        conn_pool.putconn(conn)


threads = []
for i in range(10):
    t = threading.Thread(target=worker, args=(1,))
    threads.append(t)
    t.start()

for t in threads:
    t.join()

conn_pool.closeall()
