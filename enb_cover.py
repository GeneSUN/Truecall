from datetime import datetime, timedelta, date

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, rank, dense_rank, sum, concat_ws, col, split, concat_ws, lit ,udf,count, max,lit,avg, when,concat_ws,to_date,explode

from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import mgrs
from pyspark.sql.types import (DateType, DoubleType, StringType, StructType, StructField) 

def convert_to_mgrs(latitude, longitude, MGRSPrecision =3):
    try:
        mgrs_value = mgrs.MGRS().toMGRS(latitude, longitude,MGRSPrecision =MGRSPrecision)
        return mgrs_value
    except:
        return None

def process_csv_files(date_range, file_path_pattern,partitionNum,sample_percentage = 1, dropduplicate = False): 

    """ 
    Reads CSV files from HDFS for the given date range and file path pattern and processes them.
    Args: 
        date_range (list): List of date strings in the format 'YYYY-MM-DD'. 
        file_path_pattern (str): File path pattern with a placeholder for the date, e.g., "/user/.../{}.csv"
    Returns: 
        list: A list of processed PySpark DataFrames. 
    """ 
    
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

if __name__ == "__main__":
    spark = SparkSession.builder\
        .appName('ZheS_TrueCall')\
        .master("spark://njbbvmaspd11.nss.vzwnet.com:7077")\
        .config("spark.sql.adapative.enabled","true")\
        .getOrCreate()

    #-----------------------------------------------------------------------
    sample_perc = 1
    partitionNum = 12000
    repartion_num = 1000
    today = datetime.now().date() 
    d_range = [(today - timedelta(days=i)).strftime('%Y%m%d') for i in range(1, 8)]
    hdfs_pd = 'hdfs://njbbvmaspd11.nss.vzwnet.com:9000'
    file_path_pattern = hdfs_pd + "/user/jennifer/truecall/TrueCall_VMB/UTC_date={}/"  
    df_list = process_csv_files(d_range, file_path_pattern, partitionNum, sample_percentage=sample_perc)
    df_trc_sampled = union_df_list(df_list)

    mgrs_udf = udf(convert_to_mgrs, StringType() ) 
    df_enb_mgrs = df_trc_sampled.withColumn('gridid', mgrs_udf(df_trc_sampled['end_latitude'],df_trc_sampled['end_longitude']))\
                                .select("current_enodeb_id","gridid")
    #-----------------------------------------------------------------------
    window_spec = Window.partitionBy("gridid") 

    count_df = df_enb_mgrs.groupBy("gridid", "current_enodeb_id").count() 
    ranked_df = count_df.withColumn("rank", F.row_number().over( window_spec.orderBy(col("count").desc())))\
                        .withColumn("total_count", F.sum("count").over(window_spec))\
                        .withColumn("percentage", F.round( col("count")/col("total_count")*100,2 ) )
    #-----------------------------------------------------------------------
    rank_num = 10
    top_df = ranked_df.filter(col("rank") <= rank_num) 

    others_df = (ranked_df.filter(F.col("rank") > rank_num) 
                .groupBy("gridid") 
                .agg(F.sum("count").alias("count"), 
                    F.sum("percentage").alias("percentage"), 
                    F.max("total_count").alias("total_count")) 
                .withColumn("current_enodeb_id", F.lit("others"))
                .withColumn("rank", F.lit("others"))
                .select("gridid","current_enodeb_id","count","rank","total_count","percentage")
                )
    
    union_df = top_df.union( others_df ).orderBy("rank")\
                    .withColumnRenamed("current_enodeb_id","enb")\
                    .withColumnRenamed("count","cnt")\
                    .withColumnRenamed("percentage","pct")\
                    .withColumn("date_td", F.lit(d_range[0]))\
                    .select("cnt","enb","gridid","pct")
    
    output_path = f'hdfs://njbbepapa1.nss.vzwnet.com:9000/user/ZheS/enb_cover/truecall_mgrs_{d_range[-1]}_{d_range[0]}.csv' 
    
    union_df.repartition(repartion_num)\
            .write.format("csv").option("header", "true")\
            .mode("overwrite")\
            .option("compression", "gzip")\
            .save(output_path)

    #-----------------------------------------------------------------------
