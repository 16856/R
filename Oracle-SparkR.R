# http://www.wikihow.com/Use-R-Language-to-Connect-with-an-ORACLE-Database


if (nchar(Sys.getenv('SPARK_HOME')<1)){
        Sys.setenv(SPARK_HOME='/u03/hadoop/spark-2.1.0')
	library(SparkR,lib.loc=c(file.path(Sys.getenv('SPARK_HOME'),'R','lib')))
}


if (nchar(Sys.getenv('ORACLE_HOME')<1)){
        Sys.setenv(ORACLE_HOME='/home/hadoop/instantclient_11_2')
        Sys.setenv(LD_LIBRARY_PATH='/home/hadoop/instantclient_11_2')
        Sys.setenv(NLS_LANG='American_america.UTF8')
#	library(ORE)
	library(ROracle)
}


library(dplyr)
#library(RPostgreSQL)

# Spark 2.X.X Version
spark <- sparkR.session(master = "local[2]", sparkConfig = list(spark.driver.memory = "64m"))

#sc <- sparkR.init(master='local',sparkEnvir=list(spark.driver.memory="2g"))
#sqlContext <- sparkRSQL.init(sc)

drv <- dbDriver("Oracle")
con <- dbConnect(drv,user="apps",password='apps',dbname='10.0.5.91:1571/TRAIN')
sqlString <- 'select owner,object_id,created from dba_objects'
#sqlQuery <- "select userid,actionid from alerts"

# Spark 2.X.X
dbSendQuery(con,sqlString) %>% fetch(n=-1) %>% as.DataFrame() %>% showDF() -> df

#dbSendQuery(con,sqlString) %>% fetch(n=-1) %>% createDataFrame(sqlContext,.) %>% showDF() -> df
dim(df)
dbDisconnect(con)
sparkR.session.stop()
sparkR.stop()
#res <- dbSendQuery(con,sqlString) 
#data <- fetch(res,n=-1)
#df <- createDataFrame(sqlContext,data)
#showDF(df)

#sqlQuery <- "select value,clock from events"
#dbSendQuery(con,sqlQuery) %>% fetch(n=-1) %>% createDataFrame(sqlContext,.) %>% showDF() -> df
#res <- dbSendQuery(con,sqlQuery) 
#event.info <- fetch(res,n=-1)
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
