import sqlite3
import pandas as pd
import numpy as np
import datetime 
import pytz
from dateutil import parser
from datetime import timedelta
from dataConnector import load_data_from_db,get_prev_day_data,get_prev_week_data,insert_weekly_report_data

# Convert timezone from utc to the timezone of that store
def convert_timezone(store_id,timestamp_utc,time_zone_dict):
    tz = 'America/Chicago' if time_zone_dict.get('timezone_str').get(store_id) == None else time_zone_dict.get('timezone_str').get(store_id)
    return np.datetime64(timestamp_utc.tz_localize('UTC').tz_convert(tz))

# Convert timezone from the timezone of that store to UTC
def convert_timezone_from_local(store_id,timestamp_local,time_zone_dict):
    tz = 'America/Chicago' if time_zone_dict.get('timezone_str').get(store_id) == None else time_zone_dict.get('timezone_str').get(store_id)
    return np.datetime64(timestamp_local.tz_localize(tz).tz_convert('UTC'))

# Get all store data and process it to cover missing intervals and then aggregate on hourly basis on store
def preprocess_store_status(store_status_df,business_hours_df,time_zone_dict, report_date, batch, currTime):
    print('Starting preprocessing')
    # Convert all timstamps to respective local timestamps for each store
    store_status_df['timestamp_local']=store_status_df.apply(lambda x: convert_timezone(x.store_id,x.timestamp_utc,time_zone_dict),axis=1)
    store_status_df['date']=store_status_df['timestamp_local'].apply(lambda x: x.date()) 
    
    # Filter out data outside of that report date (this checks local date)
    report_date_data=store_status_df[store_status_df['date']==report_date]
    
    #Set active and inactive status wherever value is available
    report_date_data['active_status']=report_date_data['status'].apply(lambda x: 1 if x=='active' else 0)
    report_date_data['inactive_status']=report_date_data['status'].apply(lambda x: 1 if x=='inactive' else 0)

    report_date_data['day']=report_date_data['date'].apply(lambda x: x.weekday())
    
    # Merge data with business hours
    merged_df = pd.merge(report_date_data, business_hours_df, on=['store_id','day'], how='left')
    # Get time
    merged_df['start_time_local_time'] = pd.to_datetime(merged_df['start_time_local']).dt.time
    merged_df['end_time_local_time'] = pd.to_datetime(merged_df['end_time_local']).dt.time
    merged_df['timestamp_local_time'] = pd.to_datetime(merged_df['timestamp_local']).dt.time
    # Filter out rows outside of business hours
    mask = (pd.isnull(merged_df['start_time_local'])) | (merged_df['timestamp_local_time'] >= merged_df['start_time_local_time']) & (merged_df['timestamp_local_time'] <= merged_df['end_time_local_time'])
    filtered_df=merged_df[mask]
    new_df=business_hours_df.copy()
    
    # Insert all store opening and ending times as inactive (will be used when we resample data) to ensure we have data for whole business hours duration
    new_df_start=new_df.copy()
    new_df_start['active_status']=0
    new_df_start['inactive_status']=1
    new_df_start['timestamp_local']=new_df_start['start_time_local']
    new_df_start['timestamp_utc']=new_df_start.apply(lambda x: convert_timezone_from_local(x.store_id,x.start_time_local,time_zone_dict),axis=1)
    
    new_df_end=new_df.copy()
    new_df_end['active_status']=0
    new_df_end['inactive_status']=1
    new_df_end['timestamp_local']=new_df_end['end_time_local']
    new_df_end['timestamp_utc']=new_df_end.apply(lambda x: convert_timezone_from_local(x.store_id,x.end_time_local,time_zone_dict),axis=1)
        
    if not batch:
        # Filter end time only to be till current timestamp, in case we are running report- we don't want future data to be aggregated
        new_df_end=new_df_end[new_df_end['timestamp_utc']<=currTime]
    
    overall=pd.concat([filtered_df,new_df_start,new_df_end])
    overall.index=overall['timestamp_local']
    print('Data ready calling resamplers now')
    # Resample over store id and business hour set with interval of 1 Min using backfill
    # We do it over minutes so we don't lose the accuracy and hence don't directly aggregate on hour
    # Ex. we can two poll with different status with same hour, this will count both of those and not generalise
    # Backfill will ensure that all time between two polled times will get the value as recent one and so on
    # Whenever store end time row is reached, it'll have inactive status as 1, so all minutes until last polled data will get value 1 and so on. 
    resample_1=overall.groupby(['store_id','start_time_local','end_time_local']).resample('1Min').bfill()
    resample_1=resample_1.add_suffix('_rs').reset_index()
    resample_1.index=resample_1['timestamp_local']
    
    # Resample again based on hour to get hourly duration
    final_aggr=resample_1.groupby(['store_id','start_time_local','end_time_local']).resample('1H').agg({'active_status_rs':'sum','inactive_status_rs':'sum','store_id':'last'})
    print('Preprocess done')
    return final_aggr




def get_aggr_data(report_date, currTime=datetime.datetime.now(), batch=False):
    # Source all data from dbs
    time_zone_dict,business_hours_df,store_status_df=load_data_from_db(report_date)
    # Convert start time and end time for given report date(will be used when filtering out times out of business hours)
    business_hours_df['start_time_local']=pd.to_datetime(business_hours_df['start_time_local'].apply(lambda x:datetime.datetime.combine(report_date,x.time())))
    business_hours_df['end_time_local']=pd.to_datetime(business_hours_df['end_time_local'].apply(lambda x:datetime.datetime.combine(report_date,x.time())))
    
    # Get all aggregate data for given report date, this is in hourly intervals
    hourly_aggr = preprocess_store_status(store_status_df,business_hours_df,time_zone_dict,report_date, batch, currTime)
    if not hourly_aggr.empty:
        hourly_aggr=hourly_aggr.add_suffix('_rs').reset_index()
        hourly_aggr['timestamp_utc']=hourly_aggr.apply(lambda x: convert_timezone_from_local(x.store_id,x.timestamp_local,time_zone_dict),axis=1)

    return hourly_aggr


# Generate report data for a given day, 
def generate_final_report(report_date, current_time):

    # Get Hourly Aggregation of all data for that report date and until time inside store_status.
    hourly_aggr=get_aggr_data(report_date, current_time,False)
    if hourly_aggr.empty:
        raise Exception('Unable to generate report')
    start_time=current_time - timedelta(hours=0, minutes=60)

    hourly_aggr.rename(columns = {'active_status_rs_rs':'uptime_min','inactive_status_rs_rs':'downtime_min'}, inplace = True)
    
    # Filter out data outside of last hour
    mask=(hourly_aggr['timestamp_utc'] > start_time) & (hourly_aggr['timestamp_utc']<=current_time)

    report=hourly_aggr[mask].groupby('store_id').agg({'uptime_min':'sum','downtime_min':'sum'})
    report.rename(columns = {'uptime_min':'uptime_last_hour','downtime_min':'downtime_last_hour'}, inplace = True)
    print('Getting previous day data')
    # Get last day data
    prev_day_data=get_prev_day_data(report_date)
    # Get last week data
    prev_week_data=get_prev_week_data(report_date)
    # Merge data
    final_report=pd.merge(report,prev_day_data, on='store_id',how='left')
    final_report=pd.merge(final_report,prev_week_data, on='store_id',how='left')
    print('Report generation done')
    return final_report




# Function to run preprocessing of earlier day's data so that we only fetch preprocessed aggregated data when running the report
def process_prevday_data(report_date):
    
    # Get Hourly Aggregation of all data for that report date inside store_status. 
    hourly_aggr=get_aggr_data(report_date,batch=True)
    if hourly_aggr.empty:
        raise Exception('Unable to preprocess data') 
    
    # Rename and recalculate columns, row will only be present if that time is within business hours
    hourly_aggr.rename(columns = {'active_status_rs_rs':'uptime_min','inactive_status_rs_rs':'downtime_min'}, inplace = True)
    
    # Filter out any other data if present
    mask=hourly_aggr['timestamp_utc'].apply(lambda x: x.date() == report_date)

    # Group by store id and get sum of total uptime during business hours and downtime during business hours
    daily_aggr=hourly_aggr[mask].groupby('store_id').agg({'uptime_min':'sum','downtime_min':'sum'})
    # Convert minutes to hours
    daily_aggr['uptime_last_day']=daily_aggr['uptime_min']//60
    daily_aggr['downtime_last_day']=daily_aggr['downtime_min']//60
    
    del daily_aggr['uptime_min']
    del daily_aggr['downtime_min']
    
    daily_aggr['date']=report_date
    daily_aggr=daily_aggr.reset_index()
    # Store to db for future reference
    insert_weekly_report_data(report,report_date)
    return report
