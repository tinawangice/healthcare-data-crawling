import os
import pandas
import tempfile
from uuid import uuid4 as generate_random_string
from random import randint as generate_random_int
import traceback
import re
import time

import requests
import db
import logging
from seleniumrequests import Firefox

print(os.environ['PATH'])
os.environ['PATH'] += ":" + os.path.dirname(os.path.relpath(__file__))

TABLE_NAME = 'inpatient'
HOSPITAL_OPTION_LENGTH = 488
YEAR_OPTION_LENGTH = 11


browser = None
def iterate_over_hospitals_and_years(action_on_hospital_and_year_excel):
    for hospital_option_idx in range(HOSPITAL_OPTION_LENGTH):
        for year_option_idx in range(YEAR_OPTION_LENGTH):
            try:
                # open a firefox browser
                browser = Firefox()
                browser.get('http://report.oshpd.ca.gov/Index.aspx?did=PID&rid=25&FACILITYKEY=0&REPORTYEAR=0')

                # select the hospital
                hospital_select = browser.find_element_by_xpath(
                    "//select[@name='ctl00$ContentPlaceHolder1$ReportViewer1$ctl04$ctl03$ddValue']")
                hospital_option = hospital_select.find_elements_by_xpath('.//*')[1:][hospital_option_idx]
                hospital_name = hospital_option.get_attribute('value')
                print("Hospital: %s %s" % (hospital_name, hospital_option.text))
                hospital_option.click()

                # wait for browser to load new elements after click
                while True:
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

                # in the above "year_option" for loop, i assume years are from 2016 to 1996, but some hospitcals may not have some years
                if year_option_idx >= len(year_options):
                    continue

                # select the year from dropdown menu
                year_option = year_options[year_option_idx]
                year_name = year_option.get_attribute("value")
                logging.debug("Year: %s %s" % (year_name, year_option.text))
                if str(year_option.text) == '2017':
                    logging.debug("Skipping year of 2017")
                    continue
                year_option.click()

                # click view report button
                view_report_btn = browser.find_element_by_xpath("//input[@value='View Report']")
                view_report_btn.click()

                # wait for new page to load
                while True:
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
                response = browser.request('GET', download_url, find_window_handle_timeout=300, page_load_timeout=300).content
                action_on_hospital_and_year_excel(response)
            except Exception as e:
                logging.debug("Error when handling hospital-year: %s - %s; Error: %s" % (hospital_option_idx, year_option_idx, e.message))
            finally:
                try:
                    if browser:
                        browser.close()  # make sure the explorer is closed
                except:
                    pass


def wrangle_hospital_year_excel(excel_content):
    #  python's built-in tempfile module to create a temporaty file to write excel_content to, because pandas can only read from file
    #  this tempfile will be deleted by python automatically when exiting function, even when an exception occurs
    with tempfile.NamedTemporaryFile(suffix=".xls") as f:
        f.write(excel_content)
        f.flush()
        df = pandas.read_excel(f.name)
        report_year = df.iloc[5,4]
        hospital_name = df.iloc[4,4]
        hospital_id = df.iloc[9,1]
        df1 = df.drop(df.index[:16])
        df1.dropna(axis=0, how='all', inplace=True)
        df1.dropna(axis=1, how='all', inplace=True)
        df2 = pandas.DataFrame()
        df2['item'] = df1.iloc[:, 0]

        df2['data'] = df1.iloc[:,-2]
        df2.set_index('item', inplace=True)
        # df2['first half'] = df1.iloc[:, 2]
        # df2['second half'] = df1.iloc[:, 4]

        # df2.drop(['Report Period', 'Total'])
        #  Discharged/Transferred to court/law enforcement
        try:
            a_new_col = df2.get_value('Discharged/Transferred to court/law enforcement','data')
            female = df2.get_value('Female', 'data')
        except:
            a_new_col = 0
            female = 0

    return hospital_name, hospital_id, report_year, [(a_new_col, 'int'), (female, 'int')]


def write_hospital_year_data_into_db(excel_content):
    hospital_name, hospital_id, year, cleaned_data = wrangle_hospital_year_excel(excel_content=excel_content)

    sql = """
    SELECT * FROM %s WHERE hospital_id = %s AND year = %s 
    """ % (TABLE_NAME, hospital_id, year)
    res = db.run_sql_fetch_all(sql)

    record_already_exists = len(res) != 0

    if record_already_exists:
        logging.debug("Record %s-%s already exists. Deleting it!" % (hospital_name, year))
        sql = """
        DELETE FROM %s WHERE hospital_id = %s AND year = %s 
        """ % (TABLE_NAME, hospital_id, year)
        db.run_sql_no_fetch(sql)

    sql = """
    INSERT INTO %s (hospital_id, hospital_name, `year`, a_new_col, female) VALUES (%s) 
    """

    values = """
    %s, '%s', %s
    """ % (hospital_id, hospital_name, year)

    for col_data, col_type in cleaned_data:
        values += ", " + str(col_data) if col_type != 'str' else ", '%s'" % (col_data)

    sql = sql % (TABLE_NAME, values)

    db.run_sql_no_fetch(sql)


if __name__ == '__main__':
    iterate_over_hospitals_and_years(write_hospital_year_data_into_db)
