// Spark example: NYSE data from AlphaVantage

//NOTES:
//module load spark
//export SPARK_WORKER_CORES=6
//spark-shell # run to start scala spark interactive shell
//spark-shell --master local[6]


// IMPORT ###################################
//#set up spark context and configuration
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.input_file_name
import spark.sql


val conf = new SparkConf().setMaster("local[6]").setAppName("nyse")
// val sc = new SparkContext(conf) //spark context probably exists already

//###################################


//read
//val scnyse = sc.textFile("file:/projectnb2/ct-shbioinf/rharbert/malthus/avdata/*").toDF()
val scnyse = spark.read.format("csv").option("header", "true").load("file:/projectnb2/ct-shbioinf/dan606/avdata/*")
scnyse.count()


// parse stock symbol from path to files
val df = scnyse.withColumn("path", input_file_name)
df.show(false)


//Can we trim the path?
val df2 = df.withColumn("split_path", split($"path", "/")).drop("path")
val df3 = df2.selectExpr("split_path[5]", "timestamp", "open", "high", "low", "close", "adjusted_close", "volume", "dividend_amount", "split_coefficient")
df3.show(false) // show the contents of the data frame 

// experiment with filtering
val filter1 = df3.filter(col("open") <= "10") //Filter for rows with open value <= $10
filter1.show(false)
filter1.count() // 156000 rows (days observed per symbol) vs 9,000,000+ for all avdata

val filter2 = df3.filter(col("close") > col("open")) // days when close > open
filter2.show()
filter2.count()


// write output of filter1 to new csv file
filter1.coalesce(1).write.csv("spark_nyse_filter") // writes output to single file in folder spark_nyse_filter


// quit spark-shell
:q
