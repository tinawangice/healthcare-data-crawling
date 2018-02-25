import pymysql
import pandas as pd
file = '/Users/ting/Desktop/hospital data/Hosp15_util_data_FINAL.xlsx'
x1 = pd.ExcelFile(file)
df1 = x1.parse('Section 1-4')

# severname, user, password are hidden for security
# dbServerName = ''
# dbUser = ''
# dbPassword = ''

dbName = 'testdata'
charSet = "utf8mb4"
cursorType = pymysql.cursors.DictCursor

connectionObject = pymysql.connect(host=dbServerName, user=dbUser, password=dbPassword,
                                   db=dbName, charset=charSet,cursorclass=cursorType)

try:
    cursorObject = connectionObject.cursor()

    sqlQuery = "CREATE TABLE Hospital_utilize_2015(hosp_id int NOT NULL, PRIMARY KEY(hosp_id))"
    cursorObject.execute(sqlQuery)
    for x in df1.iloc([3:,1]):
        sqlQuery = 'INSERT INTO testdata.Hospital_utilize_2015 (hosp_id) VALUES (x) '
        cursorObject.execute(sqlQuery)

    sqlQuery = 'show tables'
    cursorObject.execute(sqlQuery)

    rows = cursorObject.fetchall()

    for row in rows:
        print(row)

except Exception as e:
    print('Exceptiopn occured:{}'.format(e))

finally:
    connectionObject.close()
