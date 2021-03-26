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
#dfsam = df.sample(0.001).collect()
#dfsam.count()

dfsam = sqlContext.createDataFrame(df.head(1000000), df.schema)
dfsam.count()

# resolve currency notation from string to float
df2 = dfsam.select('trip_distance', \
    'total_amount') \
  .withColumn('total_amount', fun.regexp_replace('total_amount', '[$,]', '').cast('double'))
df2.printSchema()
df2.show(10, False)

dfsam = dfsam \
  .withColumn('fare_amount', fun.regexp_replace('fare_amount', '[$,]', '').cast('double')) \
  .withColumn('tip_amount', fun.regexp_replace('tip_amount', '[$,]', '').cast('double')) \
  .withColumn('total_amount', fun.regexp_replace('total_amount', '[$,]', '').cast('double')) 


