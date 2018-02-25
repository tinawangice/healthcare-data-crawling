import logging
import tempfile
import traceback

import pandas


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


def is_number(x):
    try:
        float(x)
        return True
    except:
        return False
