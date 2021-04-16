# pyspark modeling example with avdata
# pyspark.ml
from pyspark.ml.stat import Correlation
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import GeneralizedLinearRegression
from pyspark.ml.classification import LogisticRegression

from pyspark.sql.window import Window
from pyspark.sql import SparkSession
import pyspark.sql.functions as fun

spark = SparkSession.builder \
    .master("local[8]") \
    .appName("avdata") \
    .getOrCreate()
    
df = spark.read.format("csv").options(header='True',inferSchema='True').load("avdata/*")
df.printSchema()

# get sourcefile name from input_file_name()
df = df.withColumn("path", fun.input_file_name())
regex_str = "[\/]([^\/]+[^\/]+)$" #regex to extract text after the last / or \
df = df.withColumn("sourcefile", fun.regexp_extract("path",regex_str,1))
df.show()

#######################################################################
df=df.withColumn('timestamp', fun.to_date("timestamp"))
df=df.withColumn('dayofmonth', fun.dayofmonth("timestamp"))
df=df.withColumn('days_ago', fun.datediff(fun.current_date(), "timestamp"))

windowSpec  = Window.partitionBy("sourcefile").orderBy("days_ago")
dflag=df.withColumn("lag",fun.lag("open",14).over(windowSpec))
dflag=dflag.withColumn('twoweekdiff', fun.col('lag') - fun.col('open'))


## Can features of time be used to predict whether a stock has recently lost or gained value


