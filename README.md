# healthcare-data-crawling
A script which downloads financial operation reports from OSHPD website (500 hospitals, 20 years). Raw data are cleaned and stored in to MySQL DB.

Modules applied: selenium,multithreading, pandas etc.
## Code structure
main.py is where the crawler start. 

db.py, oshpd_data_downloader.py and hospital_data_wrangler.py are used by main.py. 

Functions are indicated by names.
## OSHPD
http://report.oshpd.ca.gov/Index.aspx?did=PID&rid=25&FACILITYKEY=0&REPORTYEAR=0 

