import logging
import os
import re
import tempfile
import time
import traceback
from multiprocessing.pool import ThreadPool
from threading import Lock

import pandas
from seleniumrequests import Firefox as browser_type

import db
from db import TABLE_NAME


# add current dir to env PATH so that selenium can find Firefox driver
os.environ['PATH'] += ":" + os.path.dirname(os.path.relpath(__file__))

# there're in total 488 hospitals and about 20 years' data on OSHPD website
HOSPITAL_OPTION_LENGTH = 488

# multithreading is used to speed up crawling;
POOL_SIZE = 8

# variables/states shared among multithreads, mostly used for retry logic
# (if one hospital/year data downloading failed, it'll be retried later)
cols_already_added = set()
missing_hospital_ids = []
all_hospital_ids_in_db = {}
failed_records = []

# browser instance creation is slow; keep a pool of browsers;  will be "created & appended on need"
browser_pool = []
browser_pool_lock = Lock()


def is_number(x):
    try:
        float(x)
        return True
    except:
        return False


def get_all_hospitals_already_in_db():
    sql = """
    SELECT hospital_id, year FROM %s
    """ % TABLE_NAME
    res = db.run_sql_fetch_all(sql)
    for id, year in res:
        if id not in all_hospital_ids_in_db:
            all_hospital_ids_in_db[id] = []
        all_hospital_ids_in_db[id].append(year)


get_all_hospitals_already_in_db()


def add_browser(b):
    with browser_pool_lock:
        if b not in browser_pool and len(browser_pool) < 20:
            browser_pool.append(b)


def get_browser():
    with browser_pool_lock:
        if len(browser_pool) == 0:
            return browser_type()
        return browser_pool.pop()


def get_missing_records_of_one_hospital(hospital_option_idx):
    browser = get_browser()
    try:
        browser.get('http://report.oshpd.ca.gov/Index.aspx?did=PID&rid=25&FACILITYKEY=0&REPORTYEAR=0')

        # select the hospital
        hospital_select = browser.find_element_by_xpath(
            "//select[@name='ctl00$ContentPlaceHolder1$ReportViewer1$ctl04$ctl03$ddValue']")
        hospital_options = hospital_select.find_elements_by_xpath('.//*')[1:]
        hospital_option = hospital_options[hospital_option_idx]
        try:
            hospital_name, hospital_id = hospital_option.get_attribute('text').split('#')
            hospital_id = int('106' + hospital_id.strip())
            hospital_option.click()

            # wait for browser to load new elements after click
            t0 = time.time()
            while True:
                if time.time() - t0 > 10:
                    raise Exception("Page Time out")
                time.sleep(0.1)
                try:
                    year_select = browser.find_element_by_xpath(
                        "//select[@name='ctl00$ContentPlaceHolder1$ReportViewer1$ctl04$ctl05$ddValue']")
                    year_options = year_select.find_elements_by_xpath('.//*')[1:]
                    if not year_options:
                        continue
                    break
                except:
                    continue
            # loading finished

            for year_option in year_options:
                year = int(year_option.text)
                if str(year_option.text) == '2017':
                    logging.debug("Skipping year of 2017")
                    continue

                if hospital_id not in all_hospital_ids_in_db or year not in all_hospital_ids_in_db[hospital_id]:
                    missing_hospital_ids.append((hospital_option_idx, hospital_id, year))
                    print("missing hospital id %s %s %s" % (hospital_option_idx, hospital_id, year))
        except:
            print(traceback.format_exc())
    except:
        logging.error(traceback.format_exc())
    finally:
        add_browser(browser)


def get_missing_records():
    pool = ThreadPool(POOL_SIZE)
    try:
        res = pool.imap_unordered(get_missing_records_of_one_hospital, range(HOSPITAL_OPTION_LENGTH))
        list(res)
    finally:
        pool.close()

    with open('./missing.txt', 'w') as f:
        for record in missing_hospital_ids:
            f.write("%s %s %s\n" % tuple(record))


def process_one_hospital(args):
    hospital_option_idx, expected_hospital_id, year = map(int, args)
    browser = get_browser()
    try:
        browser.get('http://report.oshpd.ca.gov/Index.aspx?did=PID&rid=25&FACILITYKEY=0&REPORTYEAR=0')

        # select the hospital
        hospital_select = browser.find_element_by_xpath(
            "//select[@name='ctl00$ContentPlaceHolder1$ReportViewer1$ctl04$ctl03$ddValue']")
        hospital_options = hospital_select.find_elements_by_xpath('.//*')[1:]
        hospital_option = hospital_options[hospital_option_idx]
        hospital_name, hospital_id = hospital_option.get_attribute('text').split('#')
        hospital_id = int('106' + hospital_id.strip())
        if hospital_id != expected_hospital_id:
            raise Exception("Hospital id not equal to expected one: %s" % ' '.join(
                [hospital_option_idx, expected_hospital_id, year]))
        hospital_option.click()

        # wait for browser to load new elements after click
        t0 = time.time()
        while True:
            time.sleep(0.1)
            if time.time() - t0 > 20:
                raise Exception('Page timeout')
            try:
                year_select = browser.find_element_by_xpath(
                    "//select[@name='ctl00$ContentPlaceHolder1$ReportViewer1$ctl04$ctl05$ddValue']")
                year_options = year_select.find_elements_by_xpath('.//*')[1:]
                if not year_options:
                    continue
                break
            except:
                continue
        # loading finished

        # select the year from dropdown menu
        for year_option in year_options:
            if int(year_option.text) != year:
                continue
            year_option.click()
            # click view report button
            view_report_btn = browser.find_element_by_xpath("//input[@value='View Report']")
            view_report_btn.click()

            # wait for new page to load
            t0 = time.time()
            while True:
                if time.time() - t0 > 90:
                    raise Exception('Page timeout')
                time.sleep(2)
                try:
                    content_table = browser.find_element_by_id("ctl00_ContentPlaceHolder1_ReportViewer1_ctl10")
                    content = content_table.get_attribute('innerHTML')
                    if len(content) < 5000:
                        continue
                    time.sleep(2)
                except:
                    continue
                break

            # generate the download url (when you click the export entry, a new tab will be opened with this url)
            # some variables like session id are available in the current html source page
            download_url_template = r'http://report.oshpd.ca.gov/Reserved.ReportViewerWebControl.axd?ReportSession={}&Culture={}&CultureOverrides=True&UICulture={}&UICultureOverrides=True&ReportStack={}&ControlID={}&OpType=Export&FileName=FacilitySummaryReportIP&ContentDisposition=AlwaysInline&Format=Excel'

            matches = re.search(
                r'CabUrl":"/Reserved.ReportViewerWebControl.axd\?ReportSession=(\w+).*Culture=(\w+).*UICulture=(\w+).*ReportStack=(\w+).*ControlID=(\w+)',
                browser.page_source)

            download_url = download_url_template.format(*(matches.groups()))

            # download the csv file
            response = browser.request('GET', download_url, find_window_handle_timeout=300,
                                       page_load_timeout=300).content
            write_hospital_year_data_into_db(response)
            # stop any for loop; just return
            return
    except:
        logging.error("Error when handling hospital-year: %s - %s - %s; Error: %s" % (
            hospital_option_idx, expected_hospital_id, year, traceback.format_exc()))
        failed_records.append(map(int, args))
    finally:
        # return the browser to the pool
        add_browser(browser)


def iterate_over_hospitals_and_years():
    def _get_missing_record():
        with open('./missing.txt', 'r') as f:
            for line in f:
                try:
                    yield line.split()[-3:]
                except:
                    continue

    pool = ThreadPool(POOL_SIZE)
    try:
        res = pool.imap_unordered(process_one_hospital, _get_missing_record())
        list(res)
    finally:
        pool.close()

    failed_length = len(failed_records)

    with open('./missing.txt', 'w') as f:
        f.truncate()
        while len(failed_records) > 0:
            f.write("%s %s %s\n" % tuple(failed_records.pop()))

    return failed_length != 0


def wrangle_hospital_year_excel(excel_content):
    """
    python's built-in tempfile module to create a temporaty file to write excel_content to, because pandas can only
    read from file this tempfile will be deleted by python automatically when exiting function, even when an exception occurs
    """
    with tempfile.NamedTemporaryFile(suffix=".xls") as f:
        f.write(excel_content)
        f.flush()
        df = pandas.read_excel(f.name)
        report_year = df.iloc[5, 4]
        hospital_name = df.iloc[4, 4].strip()
        hospital_id = df.iloc[9, 1]
        df1 = df.drop(df.index[:16])
        df1.dropna(axis=0, how='all', inplace=True)
        df1.dropna(axis=1, how='all', inplace=True)
        df2 = pandas.DataFrame()
        df2['item'] = df1.iloc[:, 0]
        df2['data'] = df1.iloc[:, -2]
        df2 = df2[(df2['item'] != 'Total') & (df2['item'] != 'Report Period') & (df2['item'] != 'Invalid') & (
                df2['item'] != 'Unknown') & (df2['item'] != 'Other') & (df2['data'] != 'Discharges')]
        df2.dropna(axis=0, how='any', inplace=True)

        cleaned_data = []
        my_list = []
        try:
            my_list = [(item, 'number') for item in df2['item']]
        except:
            logging.debug(traceback.format_exc())

        df2.set_index('item', inplace=True)

        already_seen_data_keys = set()

        for data_key, data_type in my_list:
            col_value = [data_key.replace("'", ''), data_type, '' if data_type == 'str' else 0]
            if len(col_value[0]) > 64:
                if col_value[0].startswith('Discharged/Transferred'):
                    col_value[0] = col_value[0][23:83]
                else:
                    col_value[0] = col_value[0][0:64]
            col_value[0] = col_value[0].strip()

            if col_value[0] in already_seen_data_keys:
                continue
            already_seen_data_keys.add(col_value[0])
            try:
                x = df2.get_value(data_key, 'data')
                if is_number(x):
                    col_value[2] = float(x)
                else:
                    continue
            except:
                logging.debug('Error when extracting %s from excel for hospital %s - year %s' % (
                    data_key, hospital_name, report_year))
            cleaned_data.append(col_value)

    return hospital_name, hospital_id, report_year, cleaned_data


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


if __name__ == '__main__':
    for _ in range(POOL_SIZE):
        # create the browser instances; they'll be shared & reused by all the threads
        add_browser(browser_type())
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
        while len(browser_pool) > 0:
            try:
                browser_pool.pop().close()
            except:
                pass
    # # some testing code
    # add_browser(browser_type())
    # process_one_hospital([203, 123, 2011])
    print("All done")
