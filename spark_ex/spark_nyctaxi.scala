# Spark example
#module load spark
#export SPARK_WORKER_CORES=6
#spark-shell # run to start scala spark interactive shell

###################################
#set up spark context and configuration
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

val conf = new SparkConf().setMaster("local[6]").setAppName("CountingSheep")
val sc = new SparkContext(conf) //don't need to run this every time. Just once per user session?

###################################


val taxidata = spark.read.textFile("file:/projectnb2/ct-shbioinf/rharbert/malthus/nyctaxi/trip data/*")
val df = taxidata.toDF("line")
val j1 = df.filter(col("line").like("01/01/2018"))
j1.first()

val taxidf = sc.textFile("file:/projectnb2/ct-shbioinf/rharbert/malthus/nyctaxi/trip data/*").toDF()
