# spark for clustering

import numpy as np

# pyspark.sql
from pyspark.sql import SparkSession
import pyspark.sql.functions as fun
from pyspark.sql.types import StructType, StructField, StringType, FloatType, DoubleType, IntegerType
from pyspark.sql.functions import monotonically_increasing_id

# pyspark.ml for clustering
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler

# set up the SparkSession
# this should be overkill even for the taxi data
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
df = df.withColumn("id", monotonically_increasing_id())
# handle dates AND time
df=df.withColumn('pickup_time', fun.to_timestamp('tpep_pickup_datetime', "yyyy-MM-dd HH:mm:ss"))
df=df.withColumn('pickup_hour', fun.hour("pickup_time"))
df=df.withColumn('pickup_month', fun.month("pickup_time"))

# clustering example

#set up dataset object with features
pred_col = ["pickup_hour", "pickup_month", "trip_distance", "congestion_surcharge"]
dffeat = df.na.drop()
vector_assembler = VectorAssembler(inputCols=pred_col, outputCol='features') #Create pipeline and pass it to stages
pipeline = Pipeline(stages=[
           vector_assembler
])
df_transformed = pipeline.fit(dffeat).transform(dffeat)

# Trains a k-means model.
kmeans = KMeans().setK(2).setSeed(1)
model = kmeans.fit(df_transformed)

# Make predictions
predictions = model.transform(df_transformed)

# Evaluate clustering by computing Silhouette score
evaluator = ClusteringEvaluator()

silhouette = evaluator.evaluate(predictions)
#values closer to 1 indicate good cluster fit (BUT beware skew within data)
print("Silhouette with squared euclidean distance = " + str(silhouette))

cost = model.computeCost(predictions)
print("Within Set Sum of Squared Errors = " + str(cost))

# Shows the result.
centers = model.clusterCenters()
print("Cluster Centers: ")
for center in centers:
    print(center)
    
    
# now test the optimal value of K
# grab a coffee or a beer... this is going to take a while 

subdf = df_transformed.sample(False, 0.01, seed=1)
subdf.count()
cost = np.zeros(20)
for k in range(2,20):
    print(k)
    train = subdf.sample(False,0.1, seed=42)
    test = subdf.sample(False, 0.1, seed=42)
    kmeans = KMeans().setK(k).setSeed(1).setFeaturesCol("features")
    model = kmeans.fit(train)
    cost[k] = model.computeCost(test) 



print(cost[2:20])

# no access to a plot device, but if there was try:
# import matplotlib.pyplot as plt
# fig, ax = plt.subplots(1,1, figsize =(8,6))
# ax.plot(range(2,20),cost[2:20])
# ax.set_xlabel('k')
# ax.set_ylabel('cost')


# what is the best value of k? The smallest value of k that gets close to the minimum cost
k = 10
kmeans = KMeans().setK(k).setSeed(1).setFeaturesCol("features")
model = kmeans.fit(df_transformed)
centers = model.clusterCenters()

print("Cluster Centers: ")
for center in centers:
    print(center)



# get predicted cluster ID for full model
transformed = model.transform(df_transformed).select('id', 'prediction')
rows = transformed.collect()
print(rows[:3])

# convert to dataframe
df_pred = spark.createDataFrame(rows)
df_pred.show()

#join with original
df_pred = df_pred.join(df, 'id')
df_pred.show()


# this might also work:
final = df.withColumn('kluster', fun.col(rows))
final.show()

# from here you can look at within 
# and between cluster differences and 
# similarities to investigate the structure 
# in this multivariate space.





