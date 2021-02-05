# test some API calls

library(dplyr)
library(ggplot2)
library(sparklyr)
#this doesn't quite work with hadoop alias in ~./bashrc
#system('/projectnb/ct-shbioinf/src/hadoop-3.3.0/bin/hadoop fs -mkdir avdata')
#system('/projectnb/ct-shbioinf/src/hadoop-3.3.0/bin/hadoop fs -put /projectnb/ct-shbioinf/rharbert/malthus/data/GME_longseries.csv avdata/GME.csv')
spark_install(version = "3.0.0")
conf <- spark_config()
conf$`sparklyr.cores.local` <- 4
conf$`sparklyr.shell.driver-memory` <- "16G"
conf$spark.memory.fraction <- 0.9
options(sparklyr.java9=T) # see if this can be resolved with java config

sc <- spark_connect(master = "local", config = conf)
#test_tbl <- copy_to(sc, test)

test_read <- spark_read_csv(sc, "file:/projectnb2/ct-shbioinf/rharbert/malthus/avdata/*")

test_read %>% summarise(count = n())



# source('key_setup.R')
# test = read.csv(paste('https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&adjusted=false&symbol=GME&interval=1min&outputsize=full&apikey=', AV_KEY, '&datatype=csv', sep =''))
# test = test %>%
#   mutate(symbol='IBM')
# ggplot(data=test) +
#   geom_path(aes(x=timestamp, y=open, group=symbol))
# 
# test = read.csv(paste('https://www.alphavantage.co/query?function=TIME_SERIES_DAILY_ADJUSTED&symbol=GME&outputsize=full&apikey=', AV_KEY, '&datatype=csv', sep =''))
# test = test %>%
#   mutate(symbol='GME') %>%
#   mutate(date = as.Date(timestamp))
# 
# write.table(test, file = 'data/GME_longseries.csv')
# ggplot(data=test) +
#   geom_path(aes(x=date, y=open, group=symbol)) +
#   theme(axis.text.x = element_text(angle=90)) +
#   theme_linedraw()




