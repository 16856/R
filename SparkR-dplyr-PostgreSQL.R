if (nchar(Sys.getenv('SPARK_HOME')<1)){
	Sys.setenv(SPARK_HOME='/u03/hadoop/spark-2.1.0')
}

library(SparkR,lib.loc=c(file.path(Sys.getenv('SPARK_HOME'),'R','lib')))

library(dplyr)
library(RPostgreSQL)

# Spark 2.x.x
sparkR.session(master = "spark://10.1.100.128:7077", appName = "R Spark SQL basic example", sparkConfig = list(spark.driver.memory="2g"))

#sc <- sparkR.init(master='local',sparkEnvir=list(spark.driver.memory="2g"))
#sqlContext <- sparkRSQL.init(sc)

dbListTables <- src_postgres(dbname='zabbix',host='10.1.100.128',port='5432',user='zabbix',password='zabbix')
as.data.frame(tbl(dbListTables,from='alerts')) %>% as.DataFrame() -> df_alerts
#df_alerts <- createDataFrame(sqlContext,dbTable_alerts)
showDF(df_alerts)

sparkR.session.stop()
sparkR.stop()
