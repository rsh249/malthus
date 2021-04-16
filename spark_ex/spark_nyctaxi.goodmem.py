# spark_nyctaxi.goodmem.py
# pyspark.sql
from pyspark.sql import SparkSession
import pyspark.sql.functions as fun
from pyspark.sql.types import StructType, StructField, StringType, FloatType, DoubleType, IntegerType

SparkContext.stop()

# set up the SparkSession
spark = SparkSession.builder \
    .master("local[16]") \
    .config('spark.executor.memory', 'G') \
    .config('spark.executor.instances', 256) \
    .config('spark.driver.memory', '64G') \
    .config('spark.driver.maxResultSize', '64G') \
    .config('spark.sql.shuffle.partitions', 256) \
    .appName("nyctaxi") \
    .getOrCreate()
    
sc._conf.getAll()
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
df = spark.read.format("csv").options(header='True').schema(schema).load("nyctaxi/trip\ data/green*")
df.printSchema()
df = df.repartition(2048) #set new partitions into queue
df.count()
sam = df.sample(0.1)
dfsam = sam.collect() 

#dfsam = sqlContext.createDataFrame(df.head(10000000), df.schema) 
dfsam = sqlContext.createDataFrame(df.head(100000), df.schema) 


