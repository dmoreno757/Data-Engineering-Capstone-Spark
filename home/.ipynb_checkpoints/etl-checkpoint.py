import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, isnull, sum
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
import pandas as pd
import etl

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    '''
    Description:
    This functions creates a spark session to be used in our aws enviroment

    Arguments:
    None
    
    Returns:
    spark: session to be used.
    '''
    
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "saurfang:spark-sas7bdat:2.0.0-s_2.11,org.apache.hadoop:hadoop-aws:2.7.2") \
        .getOrCreate()
    return spark

def process_immigration_data(spark, output_data):
    '''
    Description:
    This functions processes the immigration data to read it, to clean it, and write to s3.

    Arguments:
    spark: Spark session in use
    output_data: Where to write the files at
    
    Returns:
    None
    '''
    #Import Data
    data = '../../data/18-83510-I94-Data-2016/i94_apr16_sub.sas7bdat'
    dataframe = spark.read.load(data, format = "com.github.saurfang.sas.spark")
    
    #Clean up data, these columns showed that theres alot empty
    dataframe = dataframe.drop("occup", "entdepu", "insnum")
    dataframe.dropDuplicates()

    #Check
    print("Number of Distinct rows " + str(dataframe.count()))
    dataframe.head()
    
    
    #Create View of dataframe
    dataframe.createOrReplaceTempView("immigration_data")
    immigrationDF = spark.sql("SELECT immigration_data.* FROM immigration_data")
    
    #Write to AWS
    immigrationDF.repartition(10000, "i94yr").write.mode('overwrite').partitionBy('i94yr').parquet(output_data + 'immigration')
    
    #Quality Check
    immigrationDF.show(n=5)
    immigrationDF.count()

    
def process_airport_data(spark, output_data):  
    '''
    Description:
    This functions processes the airport data to read it, to clean it, and write to s3.

    Arguments:
    spark: Spark session in use
    output_data: Where to write the files at
    
    Returns:
    None
    '''
    
    #Read into pandas
    airportDF = pd.read_csv("/home/workspace/airport-codes_csv.csv")
    
    #Clean up data
    airportDF.dropna(subset=['ident', 'coordinates'])
    airportDF.drop_duplicates()
    airportDF = airportDF.drop(columns = ["iata_code"], axis = 1)
    airportDF = airportDF.dropna(axis = 0, how = 'any')
    
    #Check
    print("Number of Distinct rows " + str(airportDF.count()))
    airportDF.head()
    
    #Update the types in the columns
    schema = StructType([StructField("ident", StringType(), True)\
                     ,StructField("type", StringType(), True)\
                     ,StructField("name", StringType(), True)\
                     ,StructField("elevation_ft", DoubleType(), True)\
                     ,StructField("continent", StringType(), True)\
                     ,StructField("iso_country", StringType(), True)\
                     ,StructField("iso_region", StringType(), True)\
                     ,StructField("municipality", StringType(), True)\
                     ,StructField("gps_code", StringType(), True)\
                     ,StructField("local_code", StringType(), True)\
                     ,StructField("coordinates", StringType(), True)])
    
    
    #Bring into spark
    airportDF = spark.createDataFrame(airportDF, schema=schema)
    
    #Create a view of the dataframe
    airportDF.createOrReplaceTempView("airport_data")
    airportDF = spark.sql("""SELECT airport_data.* FROM airport_data""")  
    
    #Write into s3
    airportDF.repartition(10000, "iso_region").write.mode('overwrite').partitionBy('iso_region').parquet(output_data + 'airplane_code')
    
    #Check
    airportDF.show(n=5)
    airportDF.count()
        
def process_us_demo_data(spark, output_data):
    '''
    Description:
    This functions processes the us Demographics data to read it, to clean it, and write to s3.

    Arguments:
    spark: Spark session in use
    output_data: Where to write the files at
    
    Returns:
    None
    '''
    # get filepath to the us demographics data file
    us_demo_data = "/home/workspace/us-cities-demographics.csv"
    
    
    # read data file
    df_us_demoData = pd.read_csv(us_demo_data, sep=";")
    
    #Rename the columns to be able to bring them in
    df_us_demoData.rename(columns = {"Median Age":"medAge", "Male Population":"malePop", "Female Population":"femPop", "Total Population":"totPop", \
                         "Number of Veterans":"numVets", "Foreign-born":"foreignBorn", "Average Household Size":"avgHouseh", \
                         "State Code":"stateCode"}, inplace = True)
    
    #Clean up data
    df_us_demoData.dropna(subset=['stateCode'])
    df_us_demoData.drop_duplicates()
    df_us_demoData[["avgHouseh"]] = df_us_demoData[["avgHouseh"]].fillna(value = 0.0)
    print("Number of Distinct rows " + str(df_us_demoData.count()))
    df_us_demoData.head()
    
    #Column type change
    schema = StructType([StructField("City", StringType(), True)\
                     ,StructField("State", StringType(), True)\
                     ,StructField("medAge", StringType(), True)\
                     ,StructField("malePop", DoubleType(), True)\
                     ,StructField("femPop", StringType(), True)\
                     ,StructField("totPop", StringType(), True)\
                     ,StructField("numVets", StringType(), True)\
                     ,StructField("foreignBorn", StringType(), True)\
                     ,StructField("avgHouseh", StringType(), True)\
                     ,StructField("stateCode", StringType(), True)\
                     ,StructField("Race", StringType(), True)\
                     ,StructField("Count", StringType(), True)])
    
    #Create a spark dataframe
    df_us_demoData = spark.createDataFrame(df_us_demoData, schema=schema)
    
    #Create a view of the data frame
    df_us_demoData.createOrReplaceTempView("demographic_data")
    df_us_demoData = spark.sql("""SELECT demographic_data.* FROM demographic_data""")
    df_us_demoDataWrite = df_us_demoData
    
    #Write into s3 
    df_us_demoDataWrite.repartition(100, "stateCode").write.mode('overwrite').partitionBy("stateCode").parquet(output_data + "us_demo")
    
    #check
    df_us_demoData.show(n=5)
    df_us_demoData.count()
    
    
def main():
    spark = create_spark_session()
    output_data = "s3a://capemrproj/"
    
    process_immigration_data(spark, output_data)
    process_airport_data(spark, output_data)
    process_us_demo_data(spark, output_data)
    
if __name__ == "__main__":
    main()