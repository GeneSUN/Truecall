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
import mgrs
from functools import reduce 
from operator import add 
from sklearn.neighbors import BallTree
from math import radians, cos, sin, asin, sqrt
import sys 
sys.path.append('/usr/apps/vmas/script/ZS/SNAP') 
from class_SNAP import SNAP_pre_enodeb, get_date_window
from MailSender import MailSender

def convert_to_mgrs(latitude, longitude, MGRSPrecision =3):
    try:
        mgrs_value = mgrs.MGRS().toMGRS(latitude, longitude,MGRSPrecision =MGRSPrecision)
        return mgrs_value
    except:
        return None
        
def read_csv_file_by_date(date, file_path_pattern): 

    file_path = file_path_pattern.format(date) 
    df = spark.read.option("header", "true").csv(file_path) 

    return df 

def process_csv_files_for_date_range(date_range, file_path_pattern): 

    df_list = list(map(lambda date: read_csv_file_by_date(date, file_path_pattern), date_range)) 
    df_list = list(filter(None, df_list)) 
    result_df = reduce(lambda df1, df2: df1.union(df2), df_list) 

    return result_df 

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
    desired_partition_number = 5000

    spark = SparkSession.builder.appName('Truecall_mgrs_ZheS')\
                                .master("spark://njbbepapa1.nss.vzwnet.com:7077")\
                                .config("spark.sql.adapative.enabled","true")\
                                .config("spark.sql.shuffle.partitions", desired_partition_number)\
                                .getOrCreate()

    mail_sender = MailSender() 
    repartion_num = 1000

    #---------------------------------------------------------------------------------
    hdfs_title = 'hdfs://njbbvmaspd11.nss.vzwnet.com:9000'
    Numerical_Column = ['call_duration','dl_mac_volume_bytes','dl_pdcp_volume_bytes','dl_tti_ue_scheduled','drb_mean_pdcp_dl_kbps','drb_mean_pdcp_ul_kbps','end_latitude','end_longitude','endc_downlink_bytes','endc_duration','endc_duration_rate','endc_sgnb_addition_attempts_count','endc_sgnb_addition_failures_count','endc_sgnb_drops_count','endc_uplink_bytes','pucch_sinr_db','pusch_sinr_db','rsrp_dbm','rsrq_db','n1_rsrp_dbm', 'n1_rsrq_db', 'n2_rsrp_dbm', 'n2_rsrq_db', 'n3_rsrp_dbm', 'n3_rsrq_db', 'mac_mean_dl_kbps', 'mac_mean_ul_kbps', 'nr_dl_sinr_db', 'nr_rsrp_dbm', 'nr_rsrq_db','rlc_pdu_dl_bytes_count','rlc_pdu_ul_bytes_count','ul_mac_volume_bytes','ul_sinr_db']
    String_Column =['tac','current_cell_id','current_dl_earfcn','current_enodeb_id','current_nr_measurement_arfcn','current_nr_measurement_cell','current_nr_measurement_pci','indoor_outdoor_indicator_name','initial_cell_id','initial_dl_earfcn','initial_enodeb_id','nr_serving_arfcn','nr_serving_pci','nr_sgnb_x2_release_cause_code','service_type_name','serving_nr_cell_id','session_data_lost_flag','subscriber_id','userequipment_imeisv', 'vendor_name', 'userequipment_manufacturer_name', 'userequipment_model_name', 'submkt']
    Date_Column = ['call_end_timestamp','call_start_timestamp','current_nr_rrc_report_time']
    #---------------------------------------------------------------------------------

    first_day = date.today() - timedelta(days=(date.today().weekday() + 7) % 7 + 7);last_day = first_day + timedelta(days=6) 
    #first_day = date.today() - timedelta(days= 7); last_day = first_day + timedelta(days=6) 
    d_range = [ (first_day + timedelta(days=x)).strftime('%Y%m%d') for x in range((last_day - first_day).days + 1)] 
    
    file_path_pattern = hdfs_title + "/user/jennifer/truecall/TrueCall_VMB/UTC_date={}/" 
    df_trc_sampled = process_csv_files_for_date_range(d_range, file_path_pattern)
    df_trc_sampled = convert_string_numerical(df_trc_sampled,String_Column+Date_Column)

    df_trc_sampled = df_trc_sampled.filter(col('end_latitude').isNotNull() ).filter(col('end_longitude').isNotNull() )
    df_trc_sampled = df_trc_sampled.withColumn("env_tag", when(col("indoor_outdoor_indicator_name") == "Indoor", "Indoor").otherwise("Outdoor"))
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

    try:
        mail_sender.send(text = time.strftime("%Y-%m-%d %H:%M:%S"),subject="Start Running process_mgrs_truecall" , send_from ="Truecall_feature@verizon.com" )
        output_path = f'hdfs://njbbepapa1.nss.vzwnet.com:9000/user/ZheS/truecall_mgrs/truecall_mgrs_feature'
        df_mgrs_feature.repartition(repartion_num)\
                        .write.format("parquet")\
                        .mode("overwrite")\
                        .save(output_path) 

        mail_sender.send(text = time.strftime("%Y-%m-%d %H:%M:%S"),subject="Finish Running process_mgrs_truecall" , send_from ="Truecall_feature@verizon.com" )

    except Exception as e:
        print(e)
        mail_sender.send(
                        subject="Error of code TrueCall.py", 
                        text=f"an error occured at pre_enodeb code: {e}",
                        send_from ="Truecall_feature@verizon.com" 
                        ) 
