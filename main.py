import os
import traceback
from multiprocessing.pool import ThreadPool

import db
from db import TABLE_NAME
from hospital_data_wrangler import wrangle_hospital_year_excel

# add current dir to env PATH so that selenium can find Firefox driver
os.environ['PATH'] += ":" + os.path.dirname(os.path.relpath(__file__))

# there're in total 488 hospitals and about 20 years' data on OSHPD website
HOSPITAL_OPTION_LENGTH = 488

# multithreading is used to speed up crawling;
POOL_SIZE = 8

# variables/states shared among multithreads, mostly used for retry logic
# (if one hospital/year data downloading failed, it'll be retried later);
cols_already_added = set()
missing_hospital_ids = []
failed_records = []

from oshpd_data_downloader import download_one_hospital_year_data, get_missing_records_of_one_hospital, \
    shutdown_browsers


def write_hospital_year_data_into_db(excel_content):
    hospital_name, hospital_id, year, cleaned_data = wrangle_hospital_year_excel(excel_content=excel_content)

    sql = """
    SELECT * FROM %s WHERE hospital_id = %s AND year = %s 
    """ % (TABLE_NAME, hospital_id, year)
    res = db.run_sql_fetch_all(sql)

    record_already_exists = len(res) != 0

    if record_already_exists:
        return

    sql = """
    INSERT INTO %s (%s) VALUES (%s) 
    """

    col_names = 'hospital_id, hospital_name, `year`'
    values = """ %s, "%s", %s """ % (hospital_id, hospital_name, year)

    try:
        for col_name, col_type, data_value in cleaned_data:
            if col_name not in cols_already_added:
                db.add_column_if_not_exists(col_name, col_type, max(32, len(str(data_value)) * 2))
                cols_already_added.add(col_name)

            values += ", " + str(data_value) if col_type != 'str' else ', "%s"' % data_value
            col_names += ", `%s`" % col_name
    except:
        a = traceback.format_exc()
        print(a)

    sql = sql % (TABLE_NAME, col_names, values)

    db.run_sql_no_fetch(sql)
    print('Done')


def get_missing_records():
    pool = ThreadPool(POOL_SIZE)
    try:
        for missing in pool.imap_unordered(get_missing_records_of_one_hospital, range(HOSPITAL_OPTION_LENGTH)):
            missing_hospital_ids.extend(missing)
    finally:
        pool.close()

    with open('./missing.txt', 'w') as f:
        for record in missing_hospital_ids:
            f.write("%s %s %s\n" % tuple(record))


def process_one_hospital_year(args):
    raw_data, fail = download_one_hospital_year_data(args)
    if fail:
        failed_records.append(fail)
    if raw_data:
        write_hospital_year_data_into_db(raw_data)


def iterate_over_hospitals_and_years():
    def _missing_records():
        with open('./missing.txt', 'r') as f:
            for line in f:
                try:
                    yield line.split()[-3:]
                except:
                    continue

    pool = ThreadPool(POOL_SIZE)
    try:
        res = pool.imap_unordered(process_one_hospital_year, _missing_records())
        list(res)
    finally:
        pool.close()

    failed_length = len(failed_records)

    with open('./missing.txt', 'w') as f:
        f.truncate()
        while len(failed_records) > 0:
            f.write("%s %s %s\n" % tuple(failed_records.pop()))

    return failed_length != 0


if __name__ == '__main__':
    try:
        """
        get_missing_records will read db and compare records in db with the hospital-year pairs on webpage
        this function writes results to "./missing.txt"; this function is super slow, so just use it as an engine-starter;
        update "./missing.txt" in runtime using the below function iterate_over_hospitals_and_years
        """
        get_missing_records()
        goon = True
        while goon:
            """
            every time after iterating over all missing hospital-year pairs, function iterate_over_hospitals_and_years
            will update the file "./missing.txt"; theoretically, we should repeat calling this function until the
            file "./missing.txt" is empty
            """
            goon = iterate_over_hospitals_and_years()
    except:
        # no worry; failed downloading will get retried repeatedly until success
        pass
    finally:
        shutdown_browsers()
    print("All done")
