# spark_nyctaxi.badmem.py

# pyspark.sql
from pyspark.sql import SparkSession
import pyspark.sql.functions as fun
from pyspark.sql.types import StructType, StructField, StringType, FloatType, DoubleType, IntegerType

# set up the SparkSession
spark = SparkSession.builder \
    .master("local[8]") \
    .appName("nyctaxi") \
    .getOrCreate()
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
df.summarise()

#dfsam = sqlContext.createDataFrame(df.head(10000000), df.schema) 

from pyspark.sql.window import Window
windowSpec  = Window.partitionBy("pu_loc_id")
dfsam = sqlContext.createDataFrame(df.head(10000000).over(windowSpec), df.schema) 




