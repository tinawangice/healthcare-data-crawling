import logging
import time

import pymysql

TABLE_NAME = 'inpatient'
DB_NAME = 'testdata'


def run_sql_no_fetch(sql):
    conn = None
    err = None
    for retry in range(3):
        try:
            conn = pymysql.connect(host='yshen-test.ctj5svng4bym.us-west-2.rds.amazonaws.com', port=3306,
                                   user='xxx', passwd='xxx', db=DB_NAME)
            cur = conn.cursor()
            cur.execute(sql)
            return conn.commit()
        except Exception as e:
            logging.debug("Error when running sql: %s" % sql)
            err = e
            time.sleep(0.2)
        finally:
            if conn is not None:
                conn.close()
    raise err


def run_sql_fetch_all(sql):
    conn = None
    err = None
    for retry in range(3):
        try:
            conn = pymysql.connect(host='yshen-test.ctj5svng4bym.us-west-2.rds.amazonaws.com', port=3306,
                                   user='sheny35', passwd='WTwt214545', db='testdata')
            cur = conn.cursor()
            cur.execute(sql)
            return cur.fetchall()
        except Exception as e:
            logging.debug("Error when running sql: %s" % sql)
            err = e
            time.sleep(0.2)
        finally:
            if conn is not None:
                conn.close()
    raise err


def add_column_if_not_exists(col_name, col_type, size=32):
    if col_exists(col_name):
        return False
    sql = """
    ALTER TABLE %s ADD COLUMN `%s` %s
    """ % (TABLE_NAME, col_name, 'VARCHAR(%s)' % size if col_type == 'str' else 'DOUBLE')
    return run_sql_no_fetch(sql)


def col_exists(col_name):
    sql = """
    SELECT * 
    FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE 
        TABLE_SCHEMA = '%s' 
    AND TABLE_NAME = '%s' 
    AND COLUMN_NAME = '%s'
    """ % (DB_NAME, TABLE_NAME, col_name)

    if run_sql_fetch_all(sql):
        return True
    return False


if __name__ == '__main__':
    a = col_exists('Acute Care')
    print(a)
