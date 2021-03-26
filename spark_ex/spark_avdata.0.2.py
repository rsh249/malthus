#pyspark code for avdata exploration
# append: 'export PYSPARK_PYTHON=python3' to .bashrc

from pyspark.sql import SparkSession
import pyspark.sql.functions as fun

spark = SparkSession.builder \
    .master("local[8]") \
    .appName("avdata") \
    .getOrCreate()
    
# warning: It is a little slower to read the data with inferSchema='True'. 
#Consider defining schema.

df = spark.read.format("csv").options(header='True',inferSchema='True').load("avdata/*")
df.printSchema()

#get a count of rows
df.count()

# get sourcefile name from input_file_name()
df2 = df.withColumn("path", fun.input_file_name())
regex_str = "[\/]([^\/]+[^\/]+)$" #regex to extract text after the last / or \
df2 = df2.withColumn("sourcefile", fun.regexp_extract("path",regex_str,1))
df2.show()


# try filter
filter1 = df2.filter(fun.col("close") > fun.col("open")) 
filter1.show()
filter1.count()


filter1.coalesce(1).write.csv("spark_nyse_filter2") 

#quit pyspark
quit()
