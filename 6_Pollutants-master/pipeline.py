from pyspark.sql import functions as F
from pyspark.sql.window import Window

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.48bbc1b6-be3f-4d82-9491-f3e867c2516a"),
    pm25_distance_agg=Input(rid="ri.foundry.main.dataset.3d739e51-4f74-4c50-89e6-3b347dc3fdd5")
)

"""
================================================================================
Author: Elliott Fisher (elliott.fisher@duke.edu), edited by Alyssa Platt
Date:   2022-09-09
Edited: 2023-01-05

Description:
1. Rolls up observations by [zip_code, date] and produces:
    - the daily average of measurements from the monitors used
    - the total number of monitors used (with non-null measurements) to calculate
      the daily average 
    - the number of monitors used per SampleDuration:
        - 24 HOUR
        - 24 HR BLK AVG
        - 1 HR

2. Creates a count of non-null measurements and measurement averages for:
   - the  30 calendar days prior to current date 
   - the 365 calendar days prior to current date

Input:
1.  'pm25_distance_agg'
Outut:
1. 'lookback_test'
================================================================================
"""

def lookback_test(pm25_distance_agg):

    # function used in following Window funtions to count seconds in a day
    days = lambda i: i * 86400 

    # Creates window by casting timestamp to long (number of seconds) for previous 30 days
    w30 = (
        Window
        .partitionBy('ZCTA')
        .orderBy(F.col("date").cast("timestamp").cast('long'))
        .rangeBetween(-days(30), -1)
    )

    # Rolling averages and counts of measurements for previous 30 days
    zamo_prev_avg_df = (
        pm25_distance_agg
        .withColumn('prev_30_day_avg',      F.avg("measurement_avg").over(w30) )    
        .withColumn('prev_30_days_w_obs',   F.count("measurement_avg").over(w30) )    
    )

    # Creates window by casting timestamp to long (number of seconds) for previous 365 days
    w365 = (
        Window
        .partitionBy('ZCTA')
        .orderBy(F.col("date").cast("timestamp").cast('long'))
        .rangeBetween(-days(365), -1)
    )

    # Rolling averages and counts of measurements for previous 365 days
    zamo_prev_counts_df = (
        zamo_prev_avg_df
        .withColumn('prev_365_day_avg',      F.avg("measurement_avg"  ).over(w365) )    
        .withColumn('prev_365_days_w_obs',   F.count("measurement_avg").over(w365) )    
    )

    
    df = (
        zamo_prev_counts_df
        .select('ZCTA'                  , 
                'date'                      , 
                "prev_30_day_avg"           ,
                "prev_30_days_w_obs"        ,
                "prev_365_day_avg"          ,
                "prev_365_days_w_obs"       )
        .withColumnRenamed('date', 'meas_date')
    )  

    return df

        

