# spark for clustering
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler

# pyspark.sql
from pyspark.sql import SparkSession, SQLContext
import pyspark.sql.functions as fun
from pyspark.sql.types import StructType, StructField, StringType, FloatType, DoubleType, IntegerType, BooleanType
from pyspark.sql.window import Window

# set up the SparkSession
spark = SparkSession.builder \
  .master("local") \
  .config('spark.master', 'local[16]') \
  .config('spark.executor.memory', '4g') \
  .config('spark.app.name', 'avdata') \
  .config('spark.cores.max', '16') \
  .config('spark.driver.memory','16g') \
  .getOrCreate()
df = spark.read.format("csv"). \
  options(header='True',inferSchema='True'). \
  load("avdata/*")
df.printSchema()

# get sourcefile name from input_file_name()
df = df.withColumn("path", fun.input_file_name())
regex_str = "[\/]([^\/]+[^\/]+)$" #regex to extract text after the last / or \
df = df.withColumn("ticker", fun.regexp_extract("path",regex_str,1))
df = df.na.drop()
df.show()


# convert time to days ago
df=df.withColumn('timestamp', fun.to_date("timestamp"))
df=df.withColumn('days_ago', fun.datediff(fun.current_date(), "timestamp"))
df=df.withColumn('days_ago', fun.datediff(fun.current_date(), "timestamp"))

# Calculate week over week change (14 day window change)
windowSpec  = Window.partitionBy("ticker").orderBy("days_ago")
dflag = df.withColumn("lag",fun.lag("open",14).over(windowSpec))
dflag = dflag.withColumn('twoweekdiff', fun.col('lag') - fun.col('open'))
dflag.show()

# Within group (ticker) calculate anomaly time periods
# filter for anomalous events (keep outliers)
statsDF = dflag.groupBy("ticker").agg(fun.mean(dflag.twoweekdiff).alias("mean"), fun.stddev(dflag.twoweekdiff).alias("stddev"))

# add columns with upper and lower limits
statsDF = statsDF.withColumn("UpperLimit", statsDF.mean + statsDF.stddev * 3).withColumn("LowerLimit", statsDF.mean - statsDF.stddev * 3)
statsDF = statsDF.withColumnRenamed("ticker","statsticker")
statsDF = statsDF.na.drop()
statsDF.show()

# join stats with dflag 
# will give a warning that we can ignore because of join on strings I think
joinDF = dflag.join(statsDF, dflag.ticker == statsDF.statsticker)
joinDF = joinDF.na.drop()


# now filter on upper and lower limits
def detect_outlier(values, UpperLimit, LowerLimit):
    # outliers are points lying below LowerLimit or above upperLimit
    return (values < LowerLimit) or (values > UpperLimit)


udf_detect_outlier = fun.udf(lambda values, UpperLimit, LowerLimit: detect_outlier(values, UpperLimit, LowerLimit), returnType=BooleanType())

outlierDF = joinDF.withColumn("isOutlier", udf_detect_outlier(joinDF.twoweekdiff, joinDF.UpperLimit, joinDF.LowerLimit)).filter("isOutlier")
outlierDF.show()
outlierDF.count()

## from here you can start to investigate the 
# outlier time points to see what was happening
# at that time. Is this part of a correction? Is
# this evidence of sudden growth and if so, why?
