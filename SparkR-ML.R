# Set this to where Spark is installed
Sys.setenv(SPARK_HOME="/u03/hadoop/spark-2.1.0")
# This line loads SparkR from the installed directory
.libPaths(c(file.path(Sys.getenv("SPARK_HOME"), "R", "lib"), .libPaths()))
library(SparkR)



# Load SpartR library from absolute path
#if (nchar(Sys.getenv('SPARK_HOME')<1)){
#        Sys.setenv(SPARK_HOME='/u03/hadoop/spark-2.1.0')
#	library(SparkR,lib.loc=c(file.path(Sys.getenv('SPARK_HOME'),'R','lib')))
#}

library(dplyr)

# Spark 2.x.x Version 
sparkR.session(master="local", appName = "R Spark SQL basic example", sparkConfig = list(spark.some.config.option = "some-value", spark.driver.memory="2g"))

#sc <- sparkR.init(master='local',sparkEnvir=list(spark.driver.memory="2g"))
#sqlContext <- sparkRSQL.init(sc)

#df <- as.data.frame(createDataFrame(sqlContext, iris))
df <- as.DataFrame(sqlContext, iris)
model <- glm(Sepal_Length ~ Sepal_Width + Species, data = df, family = "gaussian")
summary(model)
predictions <- predict(model, newData = df)
head(SparkR::select(predictions, "Sepal_Length", "prediction"))
