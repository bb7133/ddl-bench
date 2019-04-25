# -*- coding: UTF-8 -*-

import copy
import getopt
import logging
import json
import os
import random
import sys
import thread
import threading
import time
import traceback
import uuid

try:
    import pymysql
    import pymysql.err
    import pymysql.cursors
except ImportError:
    print >> sys.stderr, "Missing PyMySQL, try to use `pip install PyMySQL` to install it"
    sys.exit(-1)

import atomic
import schema


LOG_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"
DATE_FORMAT = "%m/%d/%Y %H:%M:%S %p"
USAGE = """
Usage: ...
"""

MAX_E_NUM = 10
e_num = 0

NUM_TRIGGER = 1
table_num = 0

last_ts = None

draw_graph = True

fout = open('bench.result', 'w')
fout_lock = threading.Lock()

db_cnt = atomic.AtomicInteger()


def rand_user():
    import string
    return ''.join(random.choice(string.ascii_letters + string.digits) for _ in range(16))


def rand_digits(length):
    import string
    return ''.join(random.choice(string.digits) for _ in range(6))


# TODO: A very temporary solution that get TiDB server memory usage
# import psutil
# server_p = psutil.Process(26510)


def draw():
    if draw_graph:
        import matplotlib.pyplot as plt
        import numpy as np
        x = np.linspace(0, 2 * np.pi, 50)
        plt.plot(x, np.sin(x), 'r-x', label='Sin(x)')
        plt.plot(x, np.cos(x), 'g-^', label='Cos(x)')
        plt.legend()
        plt.xlabel('Rads')
        plt.ylabel('Amplitude')
        plt.title('Sin and Cos Waves')
        plt.show()


def count(fout):
    global last_ts
    global table_num
    if last_ts is None:
        last_ts = time.time()

    table_num += 1
    if table_num % NUM_TRIGGER == 0:
        now = time.time()
        duration = int((now - last_ts) * 1000 / NUM_TRIGGER)
        r = {
                "tables": table_num,
                "duration": duration,
                "memory": get_mem()
        }
        # print "Created %d tables, average duration for last %d creations: %d ms" % (table_num, NUM_TRIGGER, duration)
        r_str = json.dumps(r)
        logging.info(r_str)

        fout.write(r_str)
        fout.write("\n")

        last_ts = now


def bench_create_table(conn):
    logging.info("Test `CREATE TABLE` benchmark")
    with conn.cursor() as cursor:
        db_name = "create_table_bench"
        logging.info("Use `%s` as test database, truncate it first" % db_name)
        sql = "DROP DATABASE IF EXISTS `%s`" % db_name
        cursor.execute(sql)
        sql = "CREATE DATABASE `%s`" % db_name
        cursor.execute(sql)
        sql = "USE `%s`" % db_name
        cursor.execute(sql)

        with open('bench.result', 'w') as fout:
            while True:
                try:
                    rand_name = str(uuid.uuid1())
                    sql = "CREATE TABLE `%s`(a int)" % rand_name
                    cursor.execute(sql)
                    count(fout)
                except (KeyboardInterrupt, SystemExit):
                    logging.info("Exit...")
                    raise
                except Exception as e:
                    report(e)


def sub_execute(login_info):
    conn = pymysql.connect(
            host=login_info["host"],
            port=login_info["port"],
            user=login_info["username"],
            password=login_info["password"],
            db=login_info["db"],
            charset='utf8mb4',
            cursorclass=pymysql.cursors.DictCursor)

    try:
        with conn.cursor() as cursor:
            # “insertPartition"
            sql = "INSERT INTO PARTITIONS (PART_ID, CREATE_TIME, LAST_ACCESS_TIME) VALUES (%s, %s, %s)" % (rand_digits(6), rand_digits(6), rand_digits(6))
            now = time.time()
            cursor.execute(sql)
            conn.commit()
            insert_partition_ts = time.time() - now

            # "insertTbls"
            now = time.time()
            sql = "INSERT INTO TBLS (TBL_ID, CREATE_TIME, LAST_ACCESS_TIME, RETENTION) VALUES (%s, %s, %s, %s)" % (rand_digits(6), rand_digits(6), rand_digits(6), rand_digits(6))
            cursor.execute(sql)
            conn.commit()
            insert_tbl_ts = time.time() - now

            # "countPartition"
            sql = "SELECT COUNT(*) FROM PARTITIONS"
            cursor.execute(sql)

            # "countTbls"
            sql = "SELECT COUNT(*) FROM TBLS"
            cursor.execute(sql)

            return insert_partition_ts, insert_tbl_ts
    finally:
        conn.close()


def bench_test_workload(login_info):
    # Connect to the database
    logging.info("Connected to [%s:%d]" % (login_info["host"], login_info["port"]))

    conn = pymysql.connect(
            host=login_info["host"],
            port=login_info["port"],
            user=login_info["username"],
            password=login_info["password"],
            db=login_info["db"],
            charset='utf8mb4',
            cursorclass=pymysql.cursors.DictCursor)

    try:
        with conn.cursor() as cursor:
            while True:
                idx = db_cnt.inc()
                db_name = "db_bench_curr_%d" % idx
                logging.info("Start MetaStore: %s" % db_name)

                # Create metastore db
                sql = "CREATE DATABASE `%s`" % db_name
                now = time.time()
                cursor.execute(sql)
                create_ts = time.time() - now
                sql = "USE `%s`" % db_name
                cursor.execute(sql)

                # Create user and grant the user only permission to that database
                uname = rand_user()
                pswrd = str(uuid.uuid1())
                sql = "CREATE USER '{0}'@'%' IDENTIFIED BY '{1}'".format(uname, pswrd);
                cursor.execute(sql)
                conn.commit()

                sql = "GRANT Select,Insert,Update,Delete ON {0}.* TO '{1}'@'%'".format(db_name, uname)
                cursor.execute(sql)
                conn.commit()

                # run hive-schema-0.13.0.mysql.sql to setup all tables, indexes (“runMysqlScript")
                now = time.time()
                cursor.execute(schema.SQL)
                run_script_ts = time.time() - now

                user_login_info = copy.copy(login_info)
                user_login_info["username"] = uname
                user_login_info["password"] = pswrd
                user_login_info["db"] = db_name

                insert_partition_ts, insert_tbl_ts = sub_execute(user_login_info)

                if idx % NUM_TRIGGER == 0:
                    r = {
                            "databases": idx,
                            "create_ts": int(create_ts * 1000),
                            "run_script_ts": int(run_script_ts * 1000),
                            "insert_partition_ts": int(insert_partition_ts * 1000),
                            "insert_tbl_ts": int(insert_tbl_ts * 1000)
                    }
                    js = json.dumps(r)
                    print js
                    with fout_lock:
                        fout.write(js)
                        fout.write("\n")

    finally:
        logging.info("Connection closed")
        conn.close()


def report(e):
    global e_num
    if isinstance(e, pymysql.err.MySQLError):
        logging.error("MySQL error [%d]: %s" % (e.args[0], e.args[1]))
    else:
        logging.error(str(e))
    e_num += 1
    if e_num >= MAX_E_NUM:
        logging.fatal("Maximum exception caught")
        raise e


if __name__ == '__main__':
    logging.basicConfig(format=LOG_FORMAT, datefmt=DATE_FORMAT, level="INFO")

    login_info = {
            "username": "root",
            "password": "",
            "host": "localhost",
            "port": 4000,
            "db": "mysql",
    }

    try:
        opts, args = getopt.getopt(sys.argv[1:],"h:p:P:u:", ["host=", "port=", "username=", "password=", "help", "draw"])
    except getopt.GetoptError:
        print >> sys.stderr, USAGE
        sys.exit(-1)
    for opt, arg in opts:
        if opt in ("--help"):
            print >> sys.stderr, USAGE
            sys.exit(-1)
        elif opt in ("-h", "--host"):
            login_info["host"] = arg
        elif opt in ("-P", "--port"):
            login_info["port"] = int(arg)
        elif opt in ("-p", "--password"):
            login_info["password"] = arg
        elif opt in ("-u", "--user"):
            login_info["username"] = arg
        elif opt in ("--draw"):
            draw_graph = True

    for _ in xrange(10):
        thread.start_new_thread(bench_test_workload, (login_info, ))

    try:
        while True:
            pass
    finally:
        fout.close()

