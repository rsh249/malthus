# spark for clustering
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler

# pyspark.sql
from pyspark.sql import SparkSession
import pyspark.sql.functions as fun
from pyspark.sql.types import StructType, StructField, StringType, FloatType, DoubleType, IntegerType
from pyspark.sql.window import Window

# set up the SparkSession
spark = SparkSession.builder \
  .master("local") \
  .config('spark.master', 'local[16]') \
  .config('spark.executor.memory', '2g') \
  .config('spark.app.name', 'avdata') \
  .config('spark.cores.max', '16') \
  .config('spark.driver.memory','2g') \
  .getOrCreate()
df = spark.read.format("csv"). \
  options(header='True',inferSchema='True'). \
  load("avdata/*")
df.printSchema()

# get sourcefile name from input_file_name()
df = df.withColumn("path", fun.input_file_name())
regex_str = "[\/]([^\/]+[^\/]+)$" #regex to extract text after the last / or \
df = df.withColumn("sourcefile", fun.regexp_extract("path",regex_str,1))
df.show()


# convert time to days ago
df=df.withColumn('timestamp', fun.to_date("timestamp"))
df=df.withColumn('days_ago', fun.datediff(fun.current_date(), "timestamp")).show()
df=df.withColumn('days_ago', fun.datediff(fun.current_date(), "timestamp"))

# Calculate week over week change (14 day window change)
windowSpec  = Window.partitionBy("sourcefile").orderBy("days_ago")
dflag=df.withColumn("lag",fun.lag("open",14).over(windowSpec))
dflag.select('sourcefile', 'lag', 'open').show(99)
dflag.withColumn('twoweekdiff', fun.col('lag') - fun.col('open')).show() 

# Within group (ticker) calculate anomaly time periods
# filter for anomalous events (keep outliers)
