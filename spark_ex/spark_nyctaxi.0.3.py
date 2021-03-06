#pyspark code for avdata exploration
# append: 'export PYSPARK_PYTHON=python3' to .bashrc

from pyspark.sql import SparkSession
import pyspark.sql.functions as fun
from pyspark.sql.types import StructType, StructField, StringType, FloatType, DoubleType, IntegerType


spark = SparkSession.builder \
    .master("local[8]") \
    .appName("nyctaxi") \
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

#sample for easier access
#dfsam = df.sample(0.001).collect() # a random sample
#dfsam.count()

dfsam = sqlContext.createDataFrame(df.head(10000), df.schema)
dfsam.count()

# resolve currency notation from string to float
dfsam = dfsam \
  .withColumn('fare_amount', fun.regexp_replace('fare_amount', '[$,]', '').cast('double')) \
  .withColumn('tip_amount', fun.regexp_replace('tip_amount', '[$,]', '').cast('double')) \
  .withColumn('total_amount', fun.regexp_replace('total_amount', '[$,]', '').cast('double')) 

dfsam.show(2)

# handle dates AND time
#  do we need fun.to_date
dftime=dfsam.withColumn('pickup_time', fun.to_timestamp('pickup_datetime', "yyyy-MM-dd HH:mm:ss"))
dftime=dftime.withColumn('pickup_hour', fun.hour("pickup_time"))
dftime.show(2)


#bin dftime.fun.col('pickup_hour') to classify 'day/night' as day between 0800 and 2000
dftime.withColumn('daynight', \
    fun.when((fun.col('pickup_hour') >= 8) & (fun.col('pickup_hour')<= 20), 'day'). \
    otherwise('night')). \
  select('pickup_hour', 'daynight'). \
  show(99)

# bin weekday weekend
dftime=dftime.withColumn("dayofweek", fun.dayofweek("pickup_time"))
dftime.withColumn('weekpart', \
    fun.when((fun.col('dayofweek') == 7) | (fun.col('dayofweek') == 1), 1). \
    otherwise(0)). \
  select('weekpart', 'dayofweek'). \
  show(99) # view
  
  
## statistics
# https://spark.apache.org/docs/latest/ml-statistics.html



  
  

