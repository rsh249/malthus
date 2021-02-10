# Spark example

#NOTES:
#module load spark
#export SPARK_WORKER_CORES=6
#spark-shell # run to start scala spark interactive shell
#spark-shell --master local[6]


###################################
#set up spark context and configuration
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.input_file_name


val conf = new SparkConf().setMaster("local[6]").setAppName("CountingSheep")
val sc = new SparkContext(conf)

###################################

//val scnyse = sc.textFile("file:/projectnb2/ct-shbioinf/rharbert/malthus/avdata/*").toDF()
val scnyse = spark.read.format("csv").option("header", "true").load("file:/projectnb2/ct-shbioinf/rharbert/malthus/avdata/*")
scnyse.count()


// parse stock symbol from path to files
val nextdf = scnyse.withColumn("path", input_file_name)
nextdf.filter(scnyse("open") <= "10").show(false) //Filter for rows with open value <= $10



