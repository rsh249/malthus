# spark modeling: classification with decision trees
#https://spark.apache.org/docs/2.3.0/ml-classification-regression.html#decision-tree-classifier
# pyspark.ml

from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.tuning import ParamGridBuilder, TrainValidationSplit
from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml.feature import StringIndexer, VectorIndexer
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

# pyspark.sql
from pyspark.sql import SparkSession
import pyspark.sql.functions as fun
from pyspark.sql.types import StructType, StructField, StringType, FloatType, DoubleType, IntegerType

# set up the SparkSession
conf = spark.sparkContext._conf.setAll(\
  [('spark.master', 'local[16]'), \
  ('spark.executor.memory', '16g'), \
  ('spark.app.name', 'nyctaxi'), \
  ('spark.cores.max', '16'), \
  ('spark.driver.memory','124g')])
sc = SparkSession.builder.config(conf=conf).getOrCreate()

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
df = sc.read.format("csv").options(header='True').schema(schema).load("nyctaxi/trip\ data/yellow*2019*01*")
df.printSchema()

# handle dates AND time
df=df.withColumn('pickup_time', fun.to_timestamp('tpep_pickup_datetime', "yyyy-MM-dd HH:mm:ss"))
df=df.withColumn('pickup_hour', fun.hour("pickup_time"))

## ML: classification with Decision Trees

# Predicting the 'payment_type' value from other features of the Taxi data
# https://www1.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf
pred_col = ["trip_distance", "pickup_hour", "passenger_count"]
resp_var = 'RateCodeID' # trip categories
dffeat = df.na.drop()
vector_assembler = VectorAssembler(inputCols=pred_col, outputCol='features') #Create pipeline and pass it to stages
pipeline = Pipeline(stages=[
           vector_assembler
])
df_transformed = pipeline.fit(dffeat).transform(dffeat)
df_input = df_transformed.select(resp_var, 'features').withColumnRenamed(resp_var, 'label')

labelIndexer = StringIndexer(inputCol="label", outputCol="indexedLabel").fit(df_input)
# Automatically identify categorical features, and index them.
# We specify maxCategories so features with > 4 distinct values are treated as continuous.
featureIndexer =\
    VectorIndexer(inputCol="features", outputCol="indexedFeatures", maxCategories=4).fit(df_input)

# Split the data into training and test sets (30% held out for testing)
(trainingData, testData) = df_input.randomSplit([0.7, 0.3])

# Train a DecisionTree model.
dt = DecisionTreeClassifier(labelCol="indexedLabel", featuresCol="indexedFeatures")

# Chain indexers and tree in a Pipeline
pipeline = Pipeline(stages=[labelIndexer, featureIndexer, dt])

# Train model.  This also runs the indexers.
model = pipeline.fit(trainingData)

# Make predictions.
predictions = model.transform(testData)

# Select example rows to display.
predictions.select("prediction", "indexedLabel", "features").show(5)

predictions.select("prediction", "indexedLabel", "features").corr('prediction', 'indexedLabel')


# Select (prediction, true label) and compute test error
evaluator = MulticlassClassificationEvaluator(
    labelCol="indexedLabel", predictionCol="prediction", metricName="accuracy")
    
    
accuracy = evaluator.evaluate(predictions)
print("Test Error = %g " % (1.0 - accuracy))

treeModel = model.stages[2]
# summary only
print(treeModel)

