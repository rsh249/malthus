# spark for clustering
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler

# pyspark.sql
from pyspark.sql import SparkSession
import pyspark.sql.functions as fun
from pyspark.sql.types import StructType, StructField, StringType, FloatType, DoubleType, IntegerType

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

# clustering
#set up dataset object with features
pred_col = ["open", "volume", "dividend_amount"]
dffeat = df.na.drop()
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

# Shows the result.
centers = model.clusterCenters()
print("Cluster Centers: ")
for center in centers:
    print(center)

