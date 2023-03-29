# Store Monitoring
Take home interview submission for loop kitchen, Senior Backend Engineer

## Implementation details:
Server created using Flask, with thread usage for supporting background report run process. Data manipulation functions used from Pandas library for majority of the logic. I have used sqlite as a database at the moment, however it should be extended to a more robust SQL database.
## Processing:
A daily process that processes previous day's data and store the output to database. This is the process_prevday_data function in reportGenerator.py.
When report is requested by user, we only process current day's data, the remaining data for previous day and week is directly pulled from the database and aggregated in order to reduce runtime and prevent reprocessing the same data multiple times.
Additional optimization could include storing data processed till current time for present day to a database or cache, and then only process the new data between last processed time and new time when next request comes.

### Steps for processing store status data:
1. Convert timestamp to local timestamp using timezone data and merge with business hours.
2. Filter out all data where the timestamp is not within that store's business hours.
3. Create separate fields for active and inactive, to prevent interpreting unavailable data as inactive.
4. Create new store status dataframe for all store's start time and end time as the timestamp and status as inactive to indicate boundary of the data, we don't need to process entire day's data in continuous format only withing the business hour chunks.
5. Merge the new data created above with actual data
6. Resample data on 1 Min with the backfill (bfill()) method to generate timeseries data with 1 Min frequency. For ex, if status was active at 9:15 and inactive at 9:20, it'll give values to all minutes between 9:15-9:20 as inactive. Using minutes in the first status helps us generate the data without losing accuracy of the polling data as it's not necessarily hourly.
7. Aggregate and resample again with 1 Hour frequency and get uptime and downtime minutes as the sum() aggregator.
8. If this process is meant for batch processing, aggregate whole day's data and convert minutes to hours and store to database.
9. For the report, filter data withing the last hour and get previous day and previous week's data from the database.
10. Merge all of above and convert to csv and return the same to user.
