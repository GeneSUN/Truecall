from datetime import datetime, timedelta, date
from functools import reduce
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, rank, dense_rank, sum, concat_ws, col, split, concat_ws, lit ,udf,count, max,lit,avg, when,concat_ws,to_date,explode
import time
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import mgrs
from pyspark.sql.types import (DateType, DoubleType, StringType, StructType, StructField) 
import sys 
sys.path.append('/usr/apps/vmas/script/ZS') 
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

class gnb_cover():
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
        self.df_gnb_mgrs = self.df_trc.withColumn('gridid', 
                                                  self.mgrs_fun(self.df_trc['end_latitude'],
                                                           self.df_trc['end_longitude']
                                                           )
                                                )\
                                    .select("serving_nr_cell_id","gridid")
        self.ranked_df = self.get_rank()
        self.top_df = self.ranked_df.filter(col("rank") <= rank_num)
        self.others_df = (self.ranked_df.filter(F.col("rank") > rank_num) 
                        .groupBy("gridid") 
                        .agg(F.sum("count").alias("count"), 
                            F.sum("percentage").alias("percentage"), 
                            F.max("total_count").alias("total_count")) 
                        .withColumn("serving_nr_cell_id", F.lit("others"))
                        .withColumn("rank", F.lit("others"))
                        .select("gridid","serving_nr_cell_id","count","rank","total_count","percentage")
                        )
        self.union_df = self.top_df.union( self.others_df ).orderBy("rank")\
                        .withColumnRenamed("serving_nr_cell_id","gnb")\
                        .withColumnRenamed("count","cnt")\
                        .withColumnRenamed("percentage","pct")\
                        .withColumn("date_td", F.lit(self.date_str))\
                        .select("date_td","cnt","gnb","gridid","pct")\

    def get_rank(self, df_gnb_mgrs = None):
    #-----------------------------------------------------------------------
        if df_gnb_mgrs is None:
            df_gnb_mgrs = self.df_gnb_mgrs

        window_spec = Window.partitionBy("gridid") 

        count_df = df_gnb_mgrs.groupBy("gridid", "serving_nr_cell_id").count() 
        ranked_df = count_df.withColumn("rank", F.row_number().over( window_spec.orderBy(col("count").desc())))\
                            .withColumn("total_count", F.sum("count").over(window_spec))\
                            .withColumn("percentage", F.round( col("count")/col("total_count")*100,2 ) )
        return ranked_df
#-----------------------------------------------------------------------

if __name__ == "__main__":
    spark = SparkSession.builder\
                        .appName('ZheS_TrueCall_gnb')\
                        .master("spark://njbbepapa1.nss.vzwnet.com:7077")\
                        .config("spark.sql.adapative.enabled","true")\
                        .getOrCreate()
    mail_sender = MailSender() 
    hdfs_pd = 'hdfs://njbbvmaspd11.nss.vzwnet.com:9000'
    #-----------------------------------------------------------------------
    repartion_num = 1000

    last_saturday = date.today() - timedelta(days=(date.today().weekday() + 7) % 7 + 2);last_friday = last_saturday + timedelta(days=6) 
    #last_saturday = date.today() - timedelta(days= 7); last_friday = last_saturday + timedelta(days=6) 
    d_range = [ (last_saturday + timedelta(days=x)).strftime('%Y%m%d') for x in range((last_friday - last_saturday).days + 1)]
    
    file_path_pattern = hdfs_pd + "/user/jennifer/truecall/TrueCall_VMB/UTC_date={}/"  
    df_trc_sampled = process_csv_files_for_date_range(d_range, file_path_pattern)

    mgrs_udf_100 = udf(lambda lat, lon: convert_to_mgrs(lat, lon, MGRSPrecision=3), StringType()) 
    mgrs_udf_1000 = udf(lambda lat, lon: convert_to_mgrs(lat, lon, MGRSPrecision=2), StringType())
    mgrs_udf_10000 = udf(lambda lat, lon: convert_to_mgrs(lat, lon, MGRSPrecision=1), StringType()) 
    #----------------------------------------------------------------------
    
    def process_gnb_cover(spark, df, mgrs_udf, precision, date_range): 
        output_path = f"hdfs://njbbepapa1.nss.vzwnet.com:9000/user/ZheS/truecall_mgrs/gnb_cover_{precision}.csv"

        ins = gnb_cover( 
            sparksession=spark, 
            df=df, 
            mgrs_fun=mgrs_udf, 
            date_str=date_range[-1] 
        )
        ins.union_df.repartition( int(repartion_num*50/precision) )\
            .write.format("csv").option("header", "true")\
            .mode("overwrite")\
            .save(output_path) 
    
    try:
        mail_sender.send(text = time.strftime("%Y-%m-%d %H:%M:%S"),subject="Start Running process_gnb_cover100" , send_from ="Truecall_gnb@verizon.com" )
        process_gnb_cover(spark, df_trc_sampled, mgrs_udf_100, 100, d_range) 
        mail_sender.send(text = time.strftime("%Y-%m-%d %H:%M:%S"),subject="Start Running process_gnb_cover1000" , send_from ="Truecall_gnb@verizon.com" )
        process_gnb_cover(spark, df_trc_sampled, mgrs_udf_1000, 1000, d_range)
        mail_sender.send(text = time.strftime("%Y-%m-%d %H:%M:%S"),subject="Start Running process_gnb_cover10000", send_from ="Truecall_gnb@verizon.com"  )
        process_gnb_cover(spark, df_trc_sampled, mgrs_udf_10000, 10000, d_range)
        mail_sender.send(text = time.strftime("%Y-%m-%d %H:%M:%S"),subject="Finish Running process_gnb_cover", send_from ="Truecall_gnb@verizon.com"  )
    except Exception as e:
        mail_sender.send(text = e, subject="process_gnb_cover failed", send_from ="Truecall_gnb@verizon.com" )
 
    #-----------------------------------------------------------------------
