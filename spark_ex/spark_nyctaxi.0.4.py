#pyspark code for avdata exploration
# append: 'export PYSPARK_PYTHON=python3' to .bashrc
# will need to run: python -m pip install --user numpy 

# pyspark.ml
from pyspark.ml.stat import Correlation
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import GeneralizedLinearRegression
from pyspark.ml.classification import LogisticRegression

# pyspark.sql
from pyspark.sql import SparkSession
import pyspark.sql.functions as fun
from pyspark.sql.types import StructType, StructField, StringType, FloatType, DoubleType, IntegerType

# set up the SparkSession
spark = SparkSession.builder \
    .master("local[16]") \
    .appName("nyctaxi") \
    .getOrCreate()
#Need to define schema for NYC taxi data.
schema = StructType([ \
    StructField("pickup_datetime",StringType(),True), \
    StructField("dropoff_datetime",StringType(),True), \
    StructField("pu_loc_id",IntegerType(),True), \
    StructField("do_loc_id", IntegerType(), True), \
    StructField("passenger_count", IntegerType(), True), \
    StructField("trip_distance", DoubleType(), True), \
    StructField("fare_amount", StringType(), True), \
    StructField("tip_amount", StringType(), True), \
    StructField("total_amount", StringType(), True), \
    StructField("last_dropoff_datetime", StringType(), True), \
    StructField("last_do_loc_id", IntegerType(), True), \
    
  ])
df = spark.read.format("csv").options(header='True').schema(schema).load("nyctaxi/*")
df.printSchema()
#sample for easier access
#dfsam = df.sample(0.001).collect() # a random sample
dfsam = sqlContext.createDataFrame(df.head(100000), df.schema) #not a random sample
# resolve currency notation from string to float
dfsam = dfsam \
  .withColumn('fare_amount', fun.regexp_replace('fare_amount', '[$,]', '').cast('double')) \
  .withColumn('tip_amount', fun.regexp_replace('tip_amount', '[$,]', '').cast('double')) \
  .withColumn('total_amount', fun.regexp_replace('total_amount', '[$,]', '').cast('double')) 
# handle dates AND time
dftime=dfsam.withColumn('pickup_time', fun.to_timestamp('pickup_datetime', "yyyy-MM-dd HH:mm:ss"))
dftime=dftime.withColumn('pickup_hour', fun.hour("pickup_time"))
dftime.show(2)
  
  
## basic statistics
dftime.corr('total_amount', 'passenger_count')

# https://spark.apache.org/docs/latest/ml-statistics.html
# convert to vector column first
sel_col = ["trip_distance", "total_amount", "pickup_hour", "passenger_count", "tip_amount"]
dffeat = dftime.select(sel_col).na.drop()
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
# see: https://medium.com/@nutanbhogendrasharma/feature-transformer-vectorassembler-in-pyspark-ml-feature-part-3-b3c2c3c93ee9

from pyspark.ml import Pipeline
pred_col = ["trip_distance", "total_amount", "pickup_hour", "passenger_count"]
dftime = dftime.na.drop()
vector_assembler = VectorAssembler(inputCols=sel_col, outputCol='features')#Create pipeline and pass it to stages
pipeline = Pipeline(stages=[
           vector_assembler
])
df_transformed = pipeline.fit(dftime).transform(dftime).
df_transformed.show()

df_input = df_transformed.select('tip_amount', 'features').withColumnRenamed('tip_amount', 'label')
glr = GeneralizedLinearRegression(family="gaussian", link="identity", maxIter=10, regParam=0.3)

# Fit the model
model = glr.fit(df_input)
model.summary



