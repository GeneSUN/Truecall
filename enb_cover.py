from datetime import datetime, timedelta, date
import time
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, rank, dense_rank, sum, concat_ws, col, split, concat_ws, lit ,udf,count, max,lit,avg, when,concat_ws,to_date,explode
from functools import reduce
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import mgrs
from pyspark.sql.types import (DateType, DoubleType, StringType, StructType, StructField) 
import sys 
sys.path.append('/usr/apps/vmas/script/ZS/SNAP') 
sys.path.append('/usr/apps/vmas/script/ZS') 
from MailSender import MailSender
def convert_to_mgrs(latitude, longitude, MGRSPrecision =3):
    try:
        mgrs_value = mgrs.MGRS().toMGRS(latitude, longitude,MGRSPrecision =MGRSPrecision)
        return mgrs_value
    except:
        return None

def process_csv_files(date_range, file_path_pattern): 

    """ 
    Reads CSV files from HDFS for the given date range and file path pattern and processes them.
    Args: 
        date_range (list): List of date strings in the format 'YYYY-MM-DD'. 
        file_path_pattern (str): File path pattern with a placeholder for the date, e.g., "/user/.../{}.csv"
    Returns: 
        list: A list of processed PySpark DataFrames. 
    """ 
    def process_csv(date):  

        file_path = file_path_pattern.format(date)  

        df_kpis = spark.read.option("header", "true").csv(file_path)  

        return df_kpis  

    df_list =list(filter(None, map(process_csv, date_range))) 

    return reduce(lambda df1, df2: df1.union(df2), df_list)  


class enb_cover():
    global rank_num
    rank_num = 10
    def __init__(self, 
            sparksession,
            df,
            mgrs_fun,
            date_str
        ) -> None:
        self.spark = sparksession
        self.df_trc = df
        self.mgrs_fun = mgrs_fun
        self.date_str = date_str
        self.df_enb_mgrs = self.df_trc.withColumn('gridid', 
                                                  self.mgrs_fun(self.df_trc['end_latitude'],
                                                           self.df_trc['end_longitude']
                                                           )
                                                )\
                                    .select("current_enodeb_id","gridid")
        self.ranked_df = self.get_rank()
        self.top_df = self.ranked_df.filter(col("rank") <= rank_num)
        self.others_df = (self.ranked_df.filter(F.col("rank") > rank_num) 
                        .groupBy("gridid") 
                        .agg(F.sum("count").alias("count"), 
                            F.sum("percentage").alias("percentage"), 
                            F.max("total_count").alias("total_count")) 
                        .withColumn("current_enodeb_id", F.lit("others"))
                        .withColumn("rank", F.lit("others"))
                        .select("gridid","current_enodeb_id","count","rank","total_count","percentage")
                        )
        self.union_df = self.top_df.union( self.others_df ).orderBy("rank")\
                        .withColumnRenamed("current_enodeb_id","enb")\
                        .withColumnRenamed("count","cnt")\
                        .withColumnRenamed("percentage","pct")\
                        .withColumn("date_td", F.lit(self.date_str))\
                        .select("date_td","cnt","enb","gridid","pct")\

    def get_rank(self, df_enb_mgrs = None):
    #-----------------------------------------------------------------------
        if df_enb_mgrs is None:
            df_enb_mgrs = self.df_enb_mgrs

        window_spec = Window.partitionBy("gridid") 

        count_df = df_enb_mgrs.groupBy("gridid", "current_enodeb_id").count() 
        ranked_df = count_df.withColumn("rank", F.row_number().over( window_spec.orderBy(col("count").desc())))\
                            .withColumn("total_count", F.sum("count").over(window_spec))\
                            .withColumn("percentage", F.round( col("count")/col("total_count")*100,2 ) )
        return ranked_df
#-----------------------------------------------------------------------

if __name__ == "__main__":
    spark = SparkSession.builder\
        .appName('ZheS_TrueCall_enb')\
        .config("spark.sql.adapative.enabled","true")\
        .getOrCreate()
    mail_sender = MailSender() 
    #-----------------------------------------------------------------------
    last_monday = date.today() - timedelta(days=(date.today().weekday() + 7) % 7 + 7);last_sunday = last_monday + timedelta(days=6) 
    last_monday = date.today() - timedelta(days=(date.today().weekday() + 7) % 7 + 5);last_sunday = last_monday + timedelta(days=6) 

    d_range = [ (last_monday + timedelta(days=x)).strftime('%Y%m%d') for x in range((last_sunday - last_monday).days + 1)] 

    hdfs_pd = 'hdfs://njbbvmaspd11.nss.vzwnet.com:9000'
    file_path_pattern = hdfs_pd + "/user/jennifer/truecall/TrueCall_VMB/UTC_date={}/"  
    df_trc_sampled = process_csv_files(d_range, file_path_pattern)

    
    mgrs_udf_100 = udf(lambda lat, lon: convert_to_mgrs(lat, lon, MGRSPrecision=3), StringType()) 
    mgrs_udf_1000 = udf(lambda lat, lon: convert_to_mgrs(lat, lon, MGRSPrecision=2), StringType())
    mgrs_udf_10000 = udf(lambda lat, lon: convert_to_mgrs(lat, lon, MGRSPrecision=1), StringType()) 
    #----------------------------------------------------------------------
    
    def process_enb_cover(spark, df, mgrs_udf, precision, date_range): 
        output_path = f"hdfs://njbbepapa1.nss.vzwnet.com:9000/user/ZheS/truecall_mgrs/enb_cover_{precision}.csv"
        ins = enb_cover( 
            sparksession=spark, 
            df=df, 
            mgrs_fun=mgrs_udf, 
            date_str=date_range[0] 
        )
        if precision == 100:
            ins.union_df.repartition(100)\
                .write.format("csv").option("header", "true")\
                .mode("overwrite")\
                .option("compression", "gzip")\
                .save(output_path)
        else:
            ins.union_df\
                .write.format("csv").option("header", "true")\
                .mode("overwrite")\
                .option("compression", "gzip")\
                .save(output_path)
    try:
        mail_sender.send(text = time.strftime("%Y-%m-%d %H:%M:%S"),subject="Start Running enb_100" )
        process_enb_cover(spark, df_trc_sampled, mgrs_udf_100, 100, d_range)        
        mail_sender.send(text = time.strftime("%Y-%m-%d %H:%M:%S"),subject="Finish Running enb_100" )
        pass
    except Exception as e:
        print(e)
        mail_sender.send(text = e, subject="process_enb_cover_100 failed")

    try:
        mail_sender.send(text = time.strftime("%Y-%m-%d %H:%M:%S"),subject="Start Running enb_1000" )
        process_enb_cover(spark, df_trc_sampled, mgrs_udf_1000, 1000, d_range)
        mail_sender.send(text = time.strftime("%Y-%m-%d %H:%M:%S"),subject="Finish Running enb_1000" )
        pass
    except Exception as e:
        print(e)
        mail_sender.send(text = e, subject="process_enb_cover_1000 failed")

    try:
        mail_sender.send(text = time.strftime("%Y-%m-%d %H:%M:%S"),subject="Start Running enb_10000" )
        process_enb_cover(spark, df_trc_sampled, mgrs_udf_10000, 10000, d_range)
        mail_sender.send(text = time.strftime("%Y-%m-%d %H:%M:%S"),subject="Finish Running enb_10000" )
        pass
    except Exception as e:
        print(e)
        mail_sender.send(text = e, subject="process_enb_cover_10000 failed")


 
    #-----------------------------------------------------------------------
