# -*- coding: UTF-8 -*-

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

MAX_E_NUM = 10
e_num = 0

TABLE_NUM_TRIGGER = 1000
table_num = 0

last_ts = None


def count():
    global last_ts
    global table_num
    if last_ts is None:
        last_ts = time.time()

    table_num += 1
    if table_num % TABLE_NUM_TRIGGER == 0:
        now = time.time()
        cps = int(TABLE_NUM_TRIGGER / (now - last_ts))
        print "Created %d tables, cps for last %d table is: %d" % (table_num, TABLE_NUM_TRIGGER, cps)
        last_ts = now


def bench_create_table(conn):
    while True:
        try:
            with conn.cursor() as cursor:
                rand_name = uuid.uuid1()
                sql = "CREATE TABLE `%s`(a int)" % rand_name
                cursor.execute(sql)
                count()
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
    # Connect to the database
    # conn = pymysql.connect(
    #         host='10.128.11.15',
    #         port=4000,
    #         user='root',
    #         password='admin',
    #         db='create_table_bench',
    #         charset='utf8mb4',
    #         cursorclass=pymysql.cursors.DictCursor)
    conn = pymysql.connect(
            host='localhost',
            port=4000,
            user='root',
            password='',
            db='create_table_bench',
            charset='utf8mb4',
            cursorclass=pymysql.cursors.DictCursor)

    logging.info("Connected to %s" % str(conn))
    try:
        bench_create_table(conn)
    finally:
        logging.info("Connection closed")
        conn.close()
