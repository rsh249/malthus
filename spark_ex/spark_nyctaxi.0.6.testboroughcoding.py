# spark for clustering

# pyspark.sql
from pyspark.sql import SparkSession
import pyspark.sql.functions as fun
from pyspark.sql.types import StructType, StructField, StringType, FloatType, DoubleType, IntegerType
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler

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
zones = spark.read.format("csv"). \
  options(header='True'). \
  load("../../dan606/nyctaxi/misc/taxi _zone_lookup.csv")
zonesPU = zones.alias('PU').\
  withColumnRenamed("LocationID", "zPULocationID").\
  withColumnRenamed("Borough", "PUBorough").\
  withColumnRenamed("Zone", "PUZone").\
  withColumnRenamed("service_zone", "PUservice_zone")
zonesDO = zones.alias('DO').\
  withColumnRenamed("LocationID", "zDOLocationID").\
  withColumnRenamed("Borough", "DOBorough").\
  withColumnRenamed("Zone", "DOZone").\
  withColumnRenamed("service_zone", "DOservice_zone")

df = df.join(zonesPU, df.PULocationID == zonesPU.zPULocationID)
df = df.join(zonesDO, df.DOLocationID == zonesDO.zDOLocationID)

df.select('PUBorough').distinct().collect()

df = df.withColumn("PUBorough", df["PUBorough"].cast(IntegerType()))
df = df.withColumn("DOBorough", df["DOBorough"].cast(IntegerType()))

# handle dates AND time
df=df.withColumn('pickup_time', fun.to_timestamp('tpep_pickup_datetime', "yyyy-MM-dd HH:mm:ss"))
df=df.withColumn('pickup_hour', fun.hour("pickup_time"))
df=df.withColumn('pickup_month', fun.month("pickup_time"))

# clustering example

#set up dataset object with features
pred_col = ["pickup_hour", "pickup_month", "trip_distance", "", "DOBorough"]
dffeat = df
vector_assembler = VectorAssembler(inputCols=pred_col, outputCol='features') #Create pipeline and pass it to stages
pipeline = Pipeline(stages=[
           vector_assembler
])
df_transformed = pipeline.fit(dffeat).transform(dffeat)

# Trains a k-means model.
kmeans = KMeans().setK(4).setSeed(1)
model = kmeans.fit(df_transformed)

# Make predictions
predictions = model.transform(df_transformed)

# Evaluate clustering by computing Silhouette score
evaluator = ClusteringEvaluator()

silhouette = evaluator.evaluate(predictions)
print("Silhouette with squared euclidean distance = " + str(silhouette))

cost = model.computeCost(predictions)
print("Within Set Sum of Squared Errors = " + str(cost))

# Shows the result.
centers = model.clusterCenters()
print("Cluster Centers: ")
for center in centers:
    print(center)


