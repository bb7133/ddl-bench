# -*- coding: UTF-8 -*-

import getopt
import logging
import json
import os
import sys
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


LOG_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"
DATE_FORMAT = "%m/%d/%Y %H:%M:%S %p"
USAGE = """
Usage: ...
"""

MAX_E_NUM = 10
e_num = 0

TABLE_NUM_TRIGGER = 1000
table_num = 0

last_ts = None

draw_graph = True


# TODO: A very temporary solution that get TiDB server memory usage
import psutil
server_p = psutil.Process(23708)


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


def get_mem():
    return server_p.memory_percent() * 80


def count(fout):
    global last_ts
    global table_num
    if last_ts is None:
        last_ts = time.time()

    table_num += 1
    if table_num % TABLE_NUM_TRIGGER == 0:
        now = time.time()
        duration = int((now - last_ts) * 1000 / TABLE_NUM_TRIGGER)
        r = {
                "tables": table_num,
                "duration": duration,
                "memory": get_mem()
        }
        # print "Created %d tables, average duration for last %d creations: %d ms" % (table_num, TABLE_NUM_TRIGGER, duration)
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
        logging.info("Benchmark start")
        with open('bench.result', 'w') as fout:
            while True:
                try:
                    rand_name = uuid.uuid1()
                    sql = "CREATE TABLE `%s`(a int)" % rand_name
                    cursor.execute(sql)
                    count(fout)
                except (KeyboardInterrupt, SystemExit):
                    logging.info("Exit...")
                    raise
                except Exception as e:
                    report(e)

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

    username = "root"
    password = "admin"
    host = "localhost"
    port = 4000

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
            host = arg
        elif opt in ("-P", "--port"):
            port = int(arg)
        elif opt in ("-p", "--password"):
            password = arg
        elif opt in ("-u", "--user"):
            username = arg
        elif opt in ("--draw"):
            draw_graph = True

    # Connect to the database
    conn = pymysql.connect(
            host=host,
            port=port,
            user=username,
            password=password,
            db='mysql',
            charset='utf8mb4',
            cursorclass=pymysql.cursors.DictCursor)

    logging.info("Connected to [%s:%d]" % (host, port))
    try:
        bench_create_table(conn)
    finally:
        logging.info("Connection closed")
        conn.close()
