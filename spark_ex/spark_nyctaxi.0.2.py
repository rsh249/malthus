#pyspark code for avdata exploration
# append: 'export PYSPARK_PYTHON=python3' to .bashrc

from pyspark.sql import SparkSession
import pyspark.sql.functions as fun
from pyspark.sql.types import StructType, StructField, StringType, FloatType, DoubleType, IntegerType


spark = SparkSession.builder \
    .master("local[6]") \
    .appName("avdata") \
    .getOrCreate()
    
# warning: It is a little slower to read the data with inferSchema='True'. 
#Need to define schema for NYC taxi data.
schema = StructType([ \
    StructField("pickup_datetime",StringType(),True), \
    StructField("dropoff_datetime",StringType(),True), \
    StructField("pu_loc_id",IntegerType(),True), \
    StructField("do_loc_id", IntegerType(), True), \
    StructField("passenger_count", IntegerType(), True), \
    StructField("trip_distance", DoubleType(), True), \
    StructField("fare_amount", StringType(), True), \
    StructField("tip_amount", StringType(), True), \
    StructField("total_amount", StringType(), True), \
    StructField("last_dropoff_datetime", StringType(), True), \
    StructField("last_do_loc_id", IntegerType(), True), \
    
  ])
  
df = spark.read.format("csv").options(header='True').schema(schema).load("nyctaxi/*")
df.printSchema()
df.show()

#get a count of rows
df.count()

# get sourcefile name from input_file_name()
df2 = df.withColumn("path", fun.input_file_name())
regex_str = "[\/]([^\/]+[^\/]+)$" #regex to extract text after the last / or \
df2 = df2.withColumn("sourcefile", fun.regexp_extract("path",regex_str,1))
df2.show()

# try filters
df2.filter(fun.col("fare_amount") >= 1000)

#quit pyspark
quit()
