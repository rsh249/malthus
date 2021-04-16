# spark modeling: regression
# pyspark.ml


from pyspark.ml import Pipeline
from pyspark.ml.stat import Correlation
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import GeneralizedLinearRegression
from pyspark.ml.classification import LogisticRegression

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
#df = sqlContext.createDataFrame(df.head(500000), df.schema) #not a random sample

df.count()
df.show()

# handle dates AND time
df=df.withColumn('pickup_time', fun.to_timestamp('tpep_pickup_datetime', "yyyy-MM-dd HH:mm:ss"))
df=df.withColumn('pickup_hour', fun.hour("pickup_time"))
df.summary().show()

## basic statistics
df.corr('total_amount', 'passenger_count')

# https://spark.apache.org/docs/latest/ml-statistics.html
# convert to vector column first
sel_col = ["trip_distance", "total_amount", "pickup_hour", "passenger_count", "tip_amount"]
dffeat = df.select(sel_col).na.drop()
assembler = VectorAssembler(inputCols=sel_col, outputCol="features")
df_vector = assembler.transform(dffeat).select('features')
# get correlation matrix
matrix1 = Correlation.corr(df_vector, "features").head()
matrix2 = Correlation.corr(df_vector, "features", "spearman").head()

# print
print(dffeat.columns)
print("Pearson correlation matrix:\n" + str(matrix1[0]))

print("Spearman correlation matrix:\n" + str(matrix2[0]))

## ML: regression
# Predicting the 'total_amount' value from other features of the Taxi data
# see: https://medium.com/@nutanbhogendrasharma/feature-transformer-vectorassembler-in-pyspark-ml-feature-part-3-b3c2c3c93ee9
pred_col = ["trip_distance", "pickup_hour", "passenger_count", "tip_amount"]
dffeat = df.na.drop()
vector_assembler = VectorAssembler(inputCols=pred_col, outputCol='features')#Create pipeline and pass it to stages
pipeline = Pipeline(stages=[
           vector_assembler
])
df_transformed = pipeline.fit(dffeat).transform(dffeat)
df_transformed.show()

df_input = df_transformed.select('total_amount', 'features').withColumnRenamed('total_amount', 'label')
glr = GeneralizedLinearRegression(family="gaussian", link="identity", maxIter=100, regParam=0.3)

# Fit the model
model = glr.fit(df_input)
model.summary

# Print the coefficients and intercept for generalized linear regression model
print("Coefficients: " + str(model.coefficients))
print("Intercept: " + str(model.intercept))


# Summarize the model over the training set and print out some metrics
summary = model.summary
print("Coefficient Standard Errors: " + str(summary.coefficientStandardErrors))
print("T Values: " + str(summary.tValues))
print("P Values: " + str(summary.pValues))
print("Dispersion: " + str(summary.dispersion))
print("Null Deviance: " + str(summary.nullDeviance))
print("Residual Degree Of Freedom Null: " + str(summary.residualDegreeOfFreedomNull))
print("Deviance: " + str(summary.deviance))
print("Residual Degree Of Freedom: " + str(summary.residualDegreeOfFreedom))
print("AIC: " + str(summary.aic))
print("Deviance Residuals: ")
summary.residuals().show()



# Now let's build up a training/testing workflow for these data
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.regression import LinearRegression
from pyspark.ml.tuning import ParamGridBuilder, TrainValidationSplit

train, test = df_input.randomSplit([0.9, 0.1], seed=12345)

glr = GeneralizedLinearRegression(family="gaussian", link="identity", maxIter=100, regParam=0.3)

# We use a ParamGridBuilder to construct a grid of parameters to search over.
# TrainValidationSplit will try all combinations of values and determine best model using
# the evaluator.
paramGrid = ParamGridBuilder()\
    .addGrid(glr.regParam, [0.1, 0.01]) \
    .addGrid(glr.fitIntercept, [False, True])\
    .build()

# In this case the estimator is simply the linear regression.
# A TrainValidationSplit requires an Estimator, a set of Estimator ParamMaps, and an Evaluator.
tvs = TrainValidationSplit(estimator=glr,
                           estimatorParamMaps=paramGrid,
                           evaluator=RegressionEvaluator(),
                           # 80% of the data will be used for training, 20% for validation.
                           trainRatio=0.8)

# Run TrainValidationSplit, and choose the best set of parameters.
model = tvs.fit(train)

# Make predictions on test data. model is the model with combination of parameters
# that performed best.
model.bestModel.transform(test)\
    .select("features", "label", "prediction")\
    .show(99)
    
model.bestModel.transform(test).corr('label', 'prediction')

