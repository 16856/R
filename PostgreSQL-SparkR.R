if (nchar(Sys.getenv('SPARK_HOME')<1)){
        Sys.setenv(SPARK_HOME='/u03/hadoop/spark-2.1.0')
	library(SparkR,lib.loc=c(file.path(Sys.getenv('SPARK_HOME'),'R','lib')))
}

# Spark 2.X.X Version
# local Mode
#spark <- sparkR.session(master = "local[4]", sparkConfig = list(spark.driver.memory = "2g"))

# Cluster Mode
spark <- sparkR.session(master = "spark://10.1.100.128:7077", sparkConfig = list(spark.driver.memory = "2g"))

# Spark 1.X.X Version
#sc <- sparkR.init(master='local',sparkEnvir=list(spark.driver.memory="2g"))
#sqlContext <- sparkRSQL.init(sc)

library(dplyr)
library(RPostgreSQL)



drv <- dbDriver("PostgreSQL")
con <- dbConnect(drv, user="zabbix", password='zabbix', dbname='zabbix', host='10.1.100.128', port=5432)
sqlQuery <- "select userid,actionid from alerts"
dbSendQuery(con,sqlQuery) %>% fetch(n=-1) %>%  as.DataFrame() %>% showDF() -> df
#data <- fetch(res,n=-1)
#df <- createDataFrame(sqlContext,data)
#showDF(df)

#sqlQuery <- "select value,clock from events"

#dbSendQuery(con,sqlQuery) %>% fetch(n=-1) %>% createDataFrame(sqlContext,.) %>% showDF() -> df
#res <- dbSendQuery(con,sqlQuery) 
#event.info <- fetch(res,n=-1)

#event.info <- dbGetQuery(con,sqlQuery)

#event.info %>% select(value, clock) %>%
#									mutate(clock = as.POSIXct(as.numeric(clock),
#												tz = "GMT",
#											origin = "1970-01-01")) -> clock2viz
#list2bind <- list()
#for(i in 1:(nrow(clock2viz)-1)) {
#	list2bind[[i]] <- data.frame(
#							times =	clock2viz$clock[i],
#							status = clock2viz$value[i+1],
#							stringsAsFactors = FALSE
#						)
#}
#library(ggplot2)
#do.call(bind_rows, list2bind) %>%
#ggplot(aes(x=times, y = status)) +
#geom_point(size = 0.1)


sparkR.session.stop()
sparkR.stop()
