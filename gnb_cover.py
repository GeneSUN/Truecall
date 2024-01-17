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

def process_csv_files(date_range, file_path_pattern,sample_percentage = 1, dropduplicate = False): 

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
                df_kpis = spark.read.option("recursiveFileLookup", "true").option("header", "true").csv(file_path)
            else:
                df_kpis = spark.read.option("recursiveFileLookup", "true").option("header", "true").csv(file_path).sample(sample_percentage)
            if dropduplicate:
                df_kpis = df_kpis.dropDuplicates()
            else:
                pass
            
            df_list.append(df_kpis)
        except:
            print(f"data missing at {file_path}")
    return df_list 

def union_df_list(df_list,partitionNum=12000):   

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
    return df_post.repartition(partitionNum)

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
        self.df_trc = df.filter(col("serving_nr_cell_id").isNotNull()) 
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
        .appName('ZheS_TrueCall')\
        .master("spark://njbbepapa1.nss.vzwnet.com:7077")\
        .config("spark.sql.adapative.enabled","true")\
        .getOrCreate()
    hdfs_pd = 'hdfs://njbbvmaspd11.nss.vzwnet.com:9000'
    #-----------------------------------------------------------------------
    partitionNum = 12000; repartion_num = 1000

    start_date = datetime(2023, 12, 17); end_date = start_date + timedelta(6)
    #start_date = date.today() - timedelta(7) ; end_date = date.today() - timedelta(1)
    d_range = [ (start_date + timedelta(days=x)).strftime('%Y%m%d') for x in range((end_date - start_date).days + 1)] 

    file_path_pattern = hdfs_pd + "/user/jennifer/truecall/TrueCall_VMB/UTC_date={}/"  
    df_list = process_csv_files(d_range, file_path_pattern)
    df_trc_sampled = union_df_list(df_list,partitionNum)

    mgrs_udf_100 = udf(lambda lat, lon: convert_to_mgrs(lat, lon, MGRSPrecision=3), StringType()) 
    mgrs_udf_1000 = udf(lambda lat, lon: convert_to_mgrs(lat, lon, MGRSPrecision=2), StringType())
    mgrs_udf_10000 = udf(lambda lat, lon: convert_to_mgrs(lat, lon, MGRSPrecision=1), StringType()) 
    #----------------------------------------------------------------------
    
    def process_gnb_cover(spark, df, mgrs_udf, precision, date_range): 
        output_path = f"hdfs://njbbepapa1.nss.vzwnet.com:9000/user/ZheS/gnb_cover_{precision}/truecall_mgrs_{date_range[0]}_{date_range[-1]}.csv"

        ins = gnb_cover( 
            sparksession=spark, 
            df=df, 
            mgrs_fun=mgrs_udf, 
            date_str=date_range[-1] 
        )
        #ins.union_df.show()
        ins.union_df.repartition( int(repartion_num*50/precision) )\
            .write.format("csv").option("header", "true")\
            .mode("overwrite")\
            .save(output_path) 
        #.option("compression", "gzip")\
        """  

        """

    #process_gnb_cover(spark, df_trc_sampled, mgrs_udf_100, 100, d_range) 
    #process_gnb_cover(spark, df_trc_sampled, mgrs_udf_1000, 1000, d_range) 
    process_gnb_cover(spark, df_trc_sampled, mgrs_udf_10000, 10000, d_range) 
 
    """
    ins100 = gnb_cover(
                    sparksession = spark,
                    df = df_trc_sampled,
                    mgrs_fun = mgrs_udf_100,
                    date_str = d_range[0]
                    )
    """
    #-----------------------------------------------------------------------
