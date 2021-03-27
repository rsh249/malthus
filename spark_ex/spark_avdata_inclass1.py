# imports
#nano ~/.bashrc
#export PYSPARK_PYTHON=python3
#export JAVA_HOME=/usr/java/latest

from pyspark.sql import SparkSession
import pyspark.sql.functions as fun

# set up SparkSession

spark = SparkSession.builder \
  .master("local[8]") \
  .appName("avdata") \
  .getOrCreate()
  

df = spark.read.format('csv').options(header="True", inferSchema="True").load("avdata/*")
df.show()

df.count()

# get sourcefile name from input_file_name()
df = df.withColumn("path", fun.input_file_name())
regex_str = "[\/]([^\/]+[^\/]+)$" #regex to extract text after the last / or \
df = df.withColumn("sourcefile", fun.regexp_extract("path",regex_str,1))
df.show()

# handle timestamp as date
df = df.withColumn("timestamp", fun.to_date("timestamp"))

df.withColumn('dayofmonth', fun.dayofmonth("timestamp")).show(2)

# days before current date
df.withColumn('days_ago', fun.datediff(fun.current_date(), "timestamp")).show(2)
