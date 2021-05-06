# pyspark.sql
from pyspark.sql import SparkSession
import pyspark.sql.functions as fun
from pyspark.sql.types import StructType, StructField, StringType, FloatType, DoubleType, IntegerType

# set up the SparkSession
spark = SparkSession.builder \
  .master("local") \
  .config('spark.master', 'local[16]') \
  .config('spark.executor.memory', '8g') \
  .config('spark.app.name', 'nyctaxi') \
  .config('spark.cores.max', '16') \
  .config('spark.driver.memory','64g') \
  .getOrCreate()

#Need to define schema for NYC taxi data.
schema = StructType([ \
    StructField('VendorID', IntegerType(), True), \
    StructField("tpep_pickup_datetime",StringType(),True), \
    StructField("tpep_dropoff_datetime",StringType(),True), \
    StructField("passenger_count", IntegerType(), True), \
    StructField("trip_distance", DoubleType(), True), \
    StructField('RateCodeID', IntegerType(), True), \
    StructField('store_and_fwd_flag', StringType(), True), \
    StructField("PULocationID",IntegerType(),True), \
    StructField("DOLocationID", IntegerType(), True), \
    StructField("payment_type", IntegerType(), True), \
    StructField("fare_amount", DoubleType(), True), \
    StructField("extra", DoubleType(), True), \
    StructField("mta_tax", DoubleType(), True), \
    StructField("tip_amount", DoubleType(), True), \
    StructField("tolls_amount", DoubleType(), True), \
    StructField("improvement_surcharge", DoubleType(), True), \
    StructField("total_amount", DoubleType(), True), \
    StructField("congestion_surcharge", DoubleType(), True)
  ])
df = spark.read.format("csv"). \
  options(header='True'). \
  schema(schema). \
  load("../../dan606/nyctaxi/trip\ data/yellow*2019*")
df.printSchema()