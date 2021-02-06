# Spark example
#module load spark
#export SPARK_WORKER_CORES=6
#spark-shell # run to start scala spark interactive shell
val nysedata = spark.read.textFile("file:/projectnb2/ct-shbioinf/rharbert/malthus/avdata/*")
val df = nysedata.toDF("line")
val j1 = df.filter(col("line").like("01/01/2018"))
j1.first()