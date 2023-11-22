from pyspark.sql.types import (DateType, DoubleType, StringType, StructType, StructField) 
from pyspark.sql import functions as F 
from pyspark.sql.functions import ( 
    abs, avg, broadcast, count, col, concat_ws, countDistinct, desc, exp, expr, explode, first, from_unixtime, 
    lpad, length, lit, max, min, rand, regexp_replace, round, sum, to_date, udf, when, 
) 
from pyspark.sql.window import Window 
from pyspark.sql import SparkSession 
from pyspark.sql import Row
from datetime import datetime, timedelta, date 
from dateutil.parser import parse
import tempfile 
import argparse 
import time 
import requests 
import pyodbc 
import numpy as np 
import pandas as pd 
import functools
import json
from operator import add 
import math
from functools import reduce 
from operator import add 
from sklearn.neighbors import BallTree
from math import radians, cos, sin, asin, sqrt

def process_csv_files(date_range, file_path_pattern,partitionNum, hdfs = 'hdfs://njbbvmaspd11.nss.vzwnet.com:9000',sample_percentage = 1, dropduplicate = False): 

    """ 
    Reads CSV files from HDFS for the given date range and file path pattern and processes them.
    Args: 
        date_range (list): List of date strings in the format 'YYYY-MM-DD'. 
        file_path_pattern (str): File path pattern with a placeholder for the date, e.g., "/user/.../{}.csv"
    Returns: 
        list: A list of processed PySpark DataFrames. 
    """ 

    file_path_pattern = hdfs + file_path_pattern
    
    df_list = [] 
    for d in date_range: 
        file_path = file_path_pattern.format(d) 
        try:
            if sample_percentage == 1:
                df_kpis = spark.read.option("recursiveFileLookup", "true").option("header", "true").csv(file_path).repartition(partitionNum)
            else:
                df_kpis = spark.read.option("recursiveFileLookup", "true").option("header", "true").csv(file_path).sample(sample_percentage).repartition(partitionNum)
            if dropduplicate:
                df_kpis = df_kpis.dropDuplicates()
            else:
                pass
            
            df_list.append(df_kpis)
        except:
            print(f"data missing at {file_path}")
    return df_list 

def union_df_list(df_list):   

    """   
    Union a list of DataFrames and apply filters to remove duplicates and null values in 'ENODEB' column.   
    Args:   
        df_list (list): List of PySpark DataFrames to be unioned.  
    Returns:  
        DataFrame: Unioned DataFrame with duplicates removed and filters applied.   

    """   
    # Initialize the result DataFrame with the first DataFrame in the list
    try:
        df_post = df_list[0]
    except Exception as e:    
        # Handle the case where data is missing for the current DataFrame (df_temp_kpi)   
        print(f"Error processing DataFrame {0}: {e}")   
        
    # Iterate through the rest of the DataFrames in the list   
    for i in range(1, len(df_list)):    
        try:    
            # Get the current DataFrame from the list   
            df_temp_kpi = df_list[i]   
            # Union the data from df_temp_kpi with df_kpis and apply filters    
            df_post = (   
                df_post.union(df_temp_kpi)   
            )   
        except Exception as e:    
            # Handle the case where data is missing for the current DataFrame (df_temp_kpi)   
            print(f"Error processing DataFrame {i}: {e}")   
            # Optionally, you can log the error for further investigation   
    return df_post

def get_date_window(start_date, end_time = None,  days = 1, direction = "forward", formation ="str"): 
    from datetime import datetime, timedelta, date
    # Calculate the end date and start_date--------------------------------------
    
    start_date = datetime.strptime(start_date, "%Y-%m-%d")
    
    if end_time is None:
        if direction.lower() == 'forward': 
            end_date = start_date + timedelta(days=days)
        elif direction.lower() == 'backward': 
            end_date = start_date - timedelta(days=days) 
        else: 
            raise ValueError("Invalid direction argument. Use 'forward' or 'backward'.")
    else:
        end_date = datetime.strptime(end_time, "%Y-%m-%d")
        if end_date > start_date:
            direction = "forward"
        else:
            direction = "backward"

    # Generate the date range and format them as strings -------------------------------
    date_list = []
    if direction.lower() == 'backward':
        while end_date <= start_date: 
            date_string = end_date.strftime("%Y-%m-%d") 
            date_list.insert(0, date_string)  # Insert at the beginning to make it descending 
            end_date += timedelta(days=1) 
                
    if direction.lower() == 'forward':
        while end_date >= start_date: 
            date_string = end_date.strftime("%Y-%m-%d") 
            date_list.insert(0, date_string)  # Insert at the beginning to make it descending 
            end_date -= timedelta(days=1) 
    
    if formation == "datetime":
        # Convert date strings to date objects using list comprehension 
        date_list = [datetime.strptime(date_string, "%Y-%m-%d").date() for date_string in date_list] 
    elif formation == "timestamp":
        # Convert date objects to timestamps using list comprehension
        date_list = [ datetime.timestamp(datetime.strptime(date_string, "%Y-%m-%d")) for date_string in date_list] 
    else:
        pass
    
    return date_list
def convert_string_numerical(df, String_typeCols_List): 
    """ 
    This function takes a PySpark DataFrame and a list of column names specified in 'String_typeCols_List'. 
    It casts the columns in the DataFrame to double type if they are not in the list, leaving other columns 
    as they are. 

    Parameters: 
    - df (DataFrame): The PySpark DataFrame to be processed. 
    - String_typeCols_List (list): A list of column names not to be cast to double. 

    Returns: 
    - DataFrame: A PySpark DataFrame with selected columns cast to double. 
    """ 
    # Cast selected columns to double, leaving others as they are 
    df = df.select([F.col(column).cast('double') if column not in String_typeCols_List else F.col(column) for column in df.columns]) 
    return df

if __name__ == "__main__":
    # the only input is the date which is used to generate 'date_range'
    spark = SparkSession.builder.appName('Truecall_mgrs').enableHiveSupport().getOrCreate()
    parser = argparse.ArgumentParser(description="Inputs for generating Post SNA Maintenance Script Trial")

    partitionNum = 12000
    sample_perc = 1

#---------------------------------------------------------------------------------
    hdfs_title = 'hdfs://njbbvmaspd11.nss.vzwnet.com:9000'
    Numerical_Column = ['call_duration','dl_mac_volume_bytes','dl_pdcp_volume_bytes','dl_tti_ue_scheduled','drb_mean_pdcp_dl_kbps','drb_mean_pdcp_ul_kbps','end_latitude','end_longitude','endc_downlink_bytes','endc_duration','endc_duration_rate','endc_sgnb_addition_attempts_count','endc_sgnb_addition_failures_count','endc_sgnb_drops_count','endc_uplink_bytes','pucch_sinr_db','pusch_sinr_db','rsrp_dbm','rsrq_db','n1_rsrp_dbm', 'n1_rsrq_db', 'n2_rsrp_dbm', 'n2_rsrq_db', 'n3_rsrp_dbm', 'n3_rsrq_db', 'mac_mean_dl_kbps', 'mac_mean_ul_kbps', 'nr_dl_sinr_db', 'nr_rsrp_dbm', 'nr_rsrq_db','rlc_pdu_dl_bytes_count','rlc_pdu_ul_bytes_count','ul_mac_volume_bytes','ul_sinr_db']
    String_Column =['tac','current_cell_id','current_dl_earfcn','current_enodeb_id','current_nr_measurement_arfcn','current_nr_measurement_cell','current_nr_measurement_pci','indoor_outdoor_indicator_name','initial_cell_id','initial_dl_earfcn','initial_enodeb_id','nr_serving_arfcn','nr_serving_pci','nr_sgnb_x2_release_cause_code','service_type_name','serving_nr_cell_id','session_data_lost_flag','subscriber_id','userequipment_imeisv', 'vendor_name', 'userequipment_manufacturer_name', 'userequipment_model_name', 'submkt']
    Date_Column = ['call_end_timestamp','call_start_timestamp','current_nr_rrc_report_time']
#---------------------------------------------------------------------------------

    today_str = datetime.now().strftime("%Y-%m-%d")
    d_range = get_date_window(today_str, days =4, direction = 'backward')
    d_range = list( map(lambda x: x.replace("-",""),d_range) )

    file_path_pattern = "/user/jennifer/truecall/TrueCall_VMB/UTC_date={}/"  
    df_list = process_csv_files(d_range, file_path_pattern,partitionNum,sample_percentage=sample_perc)
    # def process_csv_files(date_range, file_path_pattern,partitionNum, hdfs = 'hdfs://njbbvmaspd11.nss.vzwnet.com:9000', dropduplicate = False): 
    df_trc_sampled = union_df_list(df_list)

    # convert specific String column to Numerical column
    df_trc_sampled = convert_string_numerical(df_trc_sampled,String_Column+Date_Column)

    # filter Null lat-log
    df_trc_sampled = df_trc_sampled.filter(col('end_latitude').isNotNull() ).filter(col('end_longitude').isNotNull() )

    # Indoor-Outdoor
    df_trc_sampled = df_trc_sampled.withColumn("env_tag", when(col("indoor_outdoor_indicator_name") == "Indoor", "Indoor").otherwise("Outdoor"))
# Convert lat-long to mgrs---------------------------------------------------------------------------------

    # user-defined function to create mgrs/gridid
    import mgrs
    m = mgrs.MGRS()

    def convert_to_mgrs(latitude, longitude):
        try:
            mgrs_value = m.toMGRS(latitude, longitude, MGRSPrecision =3)
            return mgrs_value
        except:
            return None

    mgrs_udf = udf(convert_to_mgrs, StringType() ) 
    df_trc_mgrs = df_trc_sampled.withColumn('gridid', mgrs_udf(df_trc_sampled['end_latitude'],df_trc_sampled['end_longitude']))
#---------------------------------------------------------------------------------
    df_mgrs_feature = df_trc_mgrs.groupBy("gridid", "env_tag").agg(
                                avg("rsrp_dbm").alias("rsrp_dbm"), 
                                avg("rsrq_db").alias("rsrq_db"), 
                                avg("ul_sinr_db").alias("ul_sinr_db"), 
                                avg("qci_mean").alias("qci_mean"), 
                                sum("rlc_pdu_dl_bytes_count").alias("sum_rlc_pdu_dl_volume_bytes"),  # we only have "rlc_pdu_dl_bytes_count" 
                                sum("rlc_pdu_ul_bytes_count").alias("sum_rlc_pdu_ul_volume_bytes"), 
                                avg("rlc_dl_throughput").alias("rlc_dl_throughput_kbps"), 
                                avg("rlc_ul_throughput").alias("rlc_ul_throughput_kbps"), 
                                sum("dl_mac_volume_bytes").alias("sum_dl_mac_volume_bytes"), 
                                sum("ul_mac_volume_bytes").alias("sum_ul_mac_volume_bytes"), 
                                avg("mac_mean_ul_kbps").alias("mac_mean_ul_kbps"), 
                                avg("mac_mean_dl_kbps").alias("mac_mean_dl_kbps"), 
                                avg("drb_mean_pdcp_dl_kbps").alias("drb_mean_pdcp_dl_kbps"), 
                                avg("drb_mean_pdcp_ul_kbps").alias("drb_mean_pdcp_ul_kbps"), 
                                sum("dl_pdcp_volume_bytes").alias("sum_dl_pdcp_volume_bytes"), 
                                sum("ul_pdcp_volume_bytes").alias("sum_pdcp_volume_ul_bytes"), 
                                sum("rlc_sdu_ul_bytes_count").alias("sum_rlc_sdu_ul_volume_kbytes"), 
                                sum("rlc_pdu_dl__retransmitted_bytes_count").alias("sum_rlc_pdu_dl_retransmitted_volume_bytes"), 
                                avg("pusch_sinr_db").alias("pusch_sinr_db"),
                                countDistinct("subscriber_id").alias("unique_imsi_cnt"),
                                count("*").alias("records_cnt")
                                )\
                                .dropDuplicates(subset=["gridid", "env_tag"])\
                                .withColumn("start_week", lit( d_range[0] ))\
                                .withColumn("end_week", lit( d_range[-1] ))

#---------------------------------------------------------------------------------
    output_path = 'hdfs://njbbvmaspd11.nss.vzwnet.com:9000/user/ZheS/TrueCall/df_mgrs_feature.csv' 
    print('flag finished')
    df_mgrs_feature.coalesce(1000) .write.format("csv").option("header", "true")\
                    .mode("overwrite")\
                    .option("compression", "gzip")\
                    .save(output_path)
#---------------------------------------------------------------------------------


    """    # count total number of data
    partitionNum = 12000

    #base_path1 = "hdfs://njbbvmaspd11.nss.vzwnet.com:9000/user/jennifer/truecall/TrueCall_VMB/UTC_date=20231030/"  
    #df_trc1 = spark.read.option("recursiveFileLookup", "true").option("header", "true").csv(base_path1).repartition(partitionNum)

    today_str = datetime.now().strftime("%Y-%m-%d")
    d_range = get_date_window(today_str, days = 9, direction = 'backward')
    d_range = list( map(lambda x: x.replace("-",""),d_range) )

    file_path_pattern = "/user/jennifer/truecall/TrueCall_VMB/UTC_date={}/"  
    df_list = process_csv_files(d_range, file_path_pattern,partitionNum)
    # def process_csv_files(date_range, file_path_pattern,partitionNum, hdfs = 'hdfs://njbbvmaspd11.nss.vzwnet.com:9000', dropduplicate = False): 
    df_trc = union_df_list(df_list)
    #print(df_trc.columns)
    print("total number of data", df_trc.count())

    # 49193997505 UTC_date=20231031 
    # 49193997505 UTC_date=20231030 
    # 266789049020 UTC_date=20231027-20231031
    # 504985439804 UTC_date=20231023-20231031
    """

    """
    base_path1 = "hdfs://njbbvmaspd11.nss.vzwnet.com:9000/user/jennifer/truecall/TrueCall_VMB/UTC_date=20231106/"
    df_trc_sampled = spark.read.option("recursiveFileLookup", "true").option("header", "true").csv(base_path1).sample(sample_perc).repartition(partitionNum)
    """

    # sample_percentage = 0.1, 10 days, 173 8805 2900