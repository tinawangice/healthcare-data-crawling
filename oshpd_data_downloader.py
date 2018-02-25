import logging
import re
import time
import traceback
from threading import Lock

from crawl_inpatient_data import failed_records, missing_hospital_ids
from seleniumrequests import Firefox as browser_type

import db
from db import TABLE_NAME

all_hospital_ids_in_db = {}


def download_one_hospital_year_data(args):
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
            # stop any for loop; just return
            return response
    except:
        logging.error("Error when handling hospital-year: %s - %s - %s; Error: %s" % (
            hospital_option_idx, expected_hospital_id, year, traceback.format_exc()))
        failed_records.append(map(int, args))
    finally:
        # return the browser to the pool
        add_browser(browser)


browser_pool = []
browser_pool_lock = Lock()


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


def shutdown_browsers():
    while len(browser_pool) > 0:
        try:
            browser_pool.pop().close()
        except:
            pass