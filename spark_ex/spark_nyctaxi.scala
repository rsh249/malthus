# Spark example
#module load spark
#export SPARK_WORKER_CORES=6
#spark-shell # run to start scala spark interactive shell

###################################
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.input_file_name
import spark.sql


val conf = new SparkConf().setMaster("local[6]").setAppName("taxidata")
// val sc = new SparkContext(conf) //spark context probably exists already

###################################


val taxidata = spark.read.format("csv").option("header", "true").load("file:/projectnb2/ct-shbioinf/rharbert/malthus/nyctaxi/trip data/*")
taxidata.count()

// FIGURE OUT HOW TO REMOVE SPACES FROM COLUMN NAMES!!
var newDf = taxidata 
for(col <- taxidata.columns){ newDf = newDf.withColumnRenamed(col, col.replaceAll("\\s", "")) }
val taxidata = newDf


// get path to files
val df = taxidata.withColumn("path", input_file_name)
df.show(false)


//Can we trim the path?
val df2 = df.withColumn("split_path", split($"path", "/")).drop("path")

val df3 = df2.selectExpr("split_path[7]", "fare_amount")
df3.show(false) // show the contents of the data frame 

// experiment with filtering (directly from taxidata)

val filter1 = taxidata.filter(col(" fare_amount") >= "1000") 
filter1.show(false)
filter1.count() 

val filter2 = taxidata.filter(col("trip_distance") > 100) 
filter2.show()
filter2.count()


// write output of filter1 to new csv file
filter1.coalesce(1).write.csv("spark_nyse_filter") // writes output to single file in folder spark_nyse_filter


// quit spark-shell
:q
