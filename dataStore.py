import sqlite3
import pandas as pd
import numpy as np
import datetime 
import pytz
from dateutil import parser
from datetime import timedelta
# conn = sqlite3.connect('lk_database')

#backend file to create tables and insert data

conn = sqlite3.connect('lk_database', detect_types=sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES)
cursor = conn.cursor()

# Create Table
cursor.execute('''
            CREATE TABLE store_status (
            store_id int,
            status nvarchar(10),
            timestamp_utc timestamp
            );
               ''')

cursor.execute('''
            CREATE TABLE business_hours_utc (
            store_id int,
            day int,
            start_time_utc timestamp,
            end_time_utc timestamp
            )
               ''')

cursor.execute('''
            CREATE TABLE time_zone (
            store_id int primary key,
            timezone_str nvarchar(50)
            )
               ''')


cursor.execute('''
            CREATE TABLE weekly_report_data (
            store_id int,
            uptime_hour int,
            downtime_hour int,
            report_date date
            )
               ''')



conn.close()

store_status=pd.read_csv('store status.csv')
time_zone=pd.read_csv('timezones.csv')
business_hours=pd.read_csv('Menu hours.csv')

store_status['timestamp_utc']=pd.to_datetime(store_status['timestamp_utc'],utc=True)
business_hours['start_time_local']=pd.to_datetime(business_hours['start_time_local'])
business_hours['end_time_local']=pd.to_datetime(business_hours['end_time_local'])

conn = sqlite3.connect('lk_database')
cursor = conn.cursor()

for row in time_zone.itertuples():
    
    cursor.execute('''
                INSERT INTO time_zone (store_id, timezone_str)
                VALUES (?,?)
                ''',
                [row.store_id, 
                row.timezone_str]
                )
conn.commit()
conn.close()
import datetime
conn = sqlite3.connect("lk_database", detect_types=sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES)
cursor = conn.cursor()

for row in store_status.itertuples():
    
    cursor.execute('''
                INSERT INTO store_status (store_id, status, timestamp_utc)
                VALUES (?,?,?)
                ''',
                (row.store_id, 
                row.status,
                row.timestamp_utc.to_pydatetime().replace(tzinfo=None))
                )
conn.commit()
conn.close()
import datetime
conn = sqlite3.connect("lk_database", detect_types=sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES)
cursor = conn.cursor()

for row in business_hours.itertuples():
    
    cursor.execute('''
                INSERT INTO business_hours (store_id,day, start_time_local, end_time_local)
                VALUES (?,?,?,?)
                ''',
                (row.store_id, 
                row.day,
                row.start_time_local.to_pydatetime().replace(tzinfo=None),
                row.end_time_local.to_pydatetime().replace(tzinfo=None))
                )
conn.commit()
conn.close()