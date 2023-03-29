import sqlite3
import pandas as pd
import numpy as np
import datetime 
import pytz
from dateutil import parser
from datetime import timedelta


# Function to insert report data for a given date to the weekly_report_data table
def insert_weekly_report_data(report,report_date):
    conn = sqlite3.connect("lk_database", detect_types=sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES)
    cursor = conn.cursor()
    cursor.execute('''delete from weekly_report_data where report_date = ?;''',(report_date.strftime('%Y-%m-%d'),))
    for row in report.itertuples():
        cursor.execute('''
                INSERT INTO weekly_report_data (store_id, uptime_hour, downtime_hour, report_date)
                VALUES (?,?,?,?)
                ''',
                (row.store_id, 
                row.uptime_last_day,
                 row.downtime_last_day,
                row.date)
                )
    conn.commit()
    conn.close()
    return 0

#Read all timezone values from the time_zone table
def read_time_zone():
    conn = sqlite3.connect("D:\loop_kitchen\lk_database", detect_types=sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES)

    time_zone_df=pd.read_sql_query('select * from time_zone',conn)
    conn.close()
    time_zone_df.index=time_zone_df['store_id']
    del time_zone_df['store_id']

    time_zone_dict=time_zone_df.to_dict()
    return time_zone_df

#Read all business hours from the business_hours table for a given day of week, day is an int
def read_business_hours(day):
    conn = sqlite3.connect("D:\loop_kitchen\lk_database", detect_types=sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES)

    business_hours_df=pd.read_sql_query('select * from business_hours where day = ?',conn,params=[day])
    conn.close()

    return business_hours_df

#Read all store status for given minimum timestamp, called using prev report date, when running daily processing or for today's date when running the report
def read_store_status(min_ts):
    conn = sqlite3.connect("D:\loop_kitchen\lk_database", detect_types=sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES)

    store_status_df=pd.read_sql_query('select * from store_status where DATETIME(timestamp_utc) >= ?',conn,params=[min_ts])
    conn.close()

    return store_status_df

#Load all data
def load_data_from_db(report_date):
    time_zone_dict=read_time_zone()
    business_hours_df=read_business_hours(report_date.weekday())
    store_status_df=read_store_status(report_date)
    return (time_zone_dict,business_hours_df,store_status_df)

def read_weekly_data(report_date):
    conn = sqlite3.connect("D:\loop_kitchen\lk_database", detect_types=sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES)
    cur=conn.cursor()
    weekly_df=pd.read_sql_query('select * from weekly_report_data where report_date=?;',conn,params=[report_date.strftime('%Y-%m-%d')])
    conn.close()
    return weekly_df

def read_weekly_data_range(start_date, end_date):
    conn = sqlite3.connect("D:\loop_kitchen\lk_database", detect_types=sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES)
    cur=conn.cursor()
    weekly_df=pd.read_sql_query('select * from weekly_report_data where report_date>=? and report_date<=?;',conn,params=[start_date.strftime('%Y-%m-%d'),end_date.strftime('%Y-%m-%d')])
    conn.close()
    return weekly_df

#Get aggregate data for a previous date when running report
def get_prev_day_data(report_date):
    prev_date=report_date- datetime.timedelta(days=1)
    data= read_weekly_data(prev_date)
    del data['report_date']
    data.rename(columns = {'uptime_hour':'uptime_last_day','downtime_hour':'downtime_last_day'}, inplace = True)
    return data

#Get aggregate data for all days in previous week when running report
def get_prev_week_data(report_date):
    start_date=report_date- datetime.timedelta(days=6)
    data=read_weekly_data_range(start_date,report_date).groupby('store_id').agg({'uptime_hour':'sum','downtime_hour':'sum'}).reset_index()
    data.rename(columns = {'uptime_hour':'uptime_last_week','downtime_hour':'downtime_last_week'}, inplace = True)
    return data
