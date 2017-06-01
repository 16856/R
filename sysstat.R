library(data.table)
library(lubridate)
library(Rcpp)

# Load data.table colnames
#vmstat_header <- read.table('../log/VMSTAT_HEADERS.log', header=TRUE)
#iostat_header <- read.table('../log/IOSTAT_HEADERS.log', header=TRUE)

# Read statistics data.
#vmstat <- fread('../out/vmstat.out', col.names=names(vmstat_header),)
#iostat <- fread('../out/iostat.out', col.names=names(iostat_header))

# 加载源数据
sysstat <- fread(file='sysstat.out', 
                        sep=",", 
                        stringsAsFactors=FALSE, 
                        header=FALSE, 
                        col.names=c("datetime","name","value"), 
                        # colClasses=c("date","character","numeric")
                        colClasses=c(NA,NA,"numeric")
                        )

# 计算单个因子行数
N <- nrow(sysstat) / length(levels(as.factor(sysstat[,name])))

redo.bytes <- as.numeric();
physical.read.bytes <- as.numeric();
physical.write.bytes <- as.numeric();
physical.read.total.bytes <- as.numeric();
physical.write.total.bytes <- as.numeric();



# 向量赋值
redo.value <- sysstat[name=='redo size',value]
physical.read.value <- sysstat[name=='physical read bytes',value]
physical.write.value <- sysstat[name=='physical write bytes',value]
physical.read.total.value <- sysstat[name=='physical read total bytes',value]
physical.write.total.value <- sysstat[name=='physical write total bytes',value]


# 0. Rcpp 加速.
print("Rcpp 调用C++ 语言函数.")
sourceCpp("sysstat.cpp")
redo.bytes <- getbytes(redo.value)
physical.read.bytes <- getbytes(physical.read.value)
physical.write.bytes <- getbytes(physical.write.value)
physical.read.total.bytes <- getbytes(physical.read.total.value)
physical.write.total.bytes <- getbytes(physical.write.total.value)

# C++ 语言数值数组首元素默认赋予0值, 
# R 语言均值计算时,首元素未知,按NA处理(或略)
redo.bytes[1] <- NA
physical.read.bytes[1] <- NA
physical.write.bytes[1] <- NA
physical.read.total.bytes[1] <- NA
physical.write.total.bytes[1] <- NA

# 1.向量化方法
print("向量化赋值")
system.time(
for (i in 2:(N)) { 
        redo.bytes[i] <- redo.value[i] - redo.value[i-1] 
        physical.read.bytes[i] <- physical.read.value[i] - physical.read.value[i-1] 
        physical.write.bytes[i] <- physical.write.value[i] - physical.write.value[i-1]
        physical.read.total.bytes[i] <- physical.read.total.value[i] - physical.read.total.value[i-1] 
        physical.write.total.bytes[i] <- physical.write.total.value[i] - physical.write.total.value[i-1]
}
)

# 2.向量化并行方法
# parallel processing
#library(foreach)
#library(doSNOW)
#cl <- makeCluster(4, type="SOCK") # for 4 cores machine
#registerDoSNOW (cl)
#system.time(
#foreach (i = 2:N, .combine=c) %dopar%{ 
#        redo.bytes[i] <- redo.value[i] - redo.value[i-1] 
#        physical.read.bytes[i] <- physical.read.value[i] - physical.read.value[i-1] 
#        physical.write.bytes[i] <- physical.write.value[i] - physical.write.value[i-1]
#        physical.read.total.bytes[i] <- physical.read.total.value[i] - physical.read.total.value[i-1] 
#        physical.write.total.bytes[i] <- physical.write.total.value[i] - physical.write.total.value[i-1]
#}
#)


# 1.原始方法
#print("原始方法循环处理.")
#date()
#system.time(
#for (i in 2:(N)) { 
#       redo.bytes[i] <- sysstat[name=='redo size',][i, value] - sysstat[name=='redo size',][i-1, value] 
#       physical.read.bytes[i] <- sysstat[name=='physical read bytes',][i, value] - sysstat[name=='physical read bytes',][i-1, value] 
#       physical.write.bytes[i] <- sysstat[name=='physical write bytes',][i, value] - sysstat[name=='physical write bytes',][i-1, value] 
#       physical.read.total.bytes[i] <- sysstat[name=='physical read total bytes',][i, value] - sysstat[name=='physical read total bytes',][i-1, value] 
#       physical.write.total.bytes[i] <- sysstat[name=='physical write total bytes',][i, value] - sysstat[name=='physical write total bytes',][i-1, value] 
#}
#)
#date()


# 用均值初始化首元素
print("用均值填充首元素")
redo.bytes[1] <- round(mean(redo.bytes, na.rm = TRUE))
physical.read.bytes[1] <- round(mean(physical.read.bytes, na.rm = TRUE))
physical.write.bytes[1] <- round(mean(physical.write.bytes, na.rm = TRUE))
physical.read.total.bytes[1] <- round(mean(physical.read.total.bytes, na.rm = TRUE))
physical.write.total.bytes[1] <- round(mean(physical.write.total.bytes, na.rm = TRUE))


print(paste0("redo.bytes[1]:[",redo.bytes[1],"]"))
print(paste0("physical.read.bytes[1]:[",physical.read.bytes[1],"]"))
print(paste0("physical.write.bytes[1]:[",physical.write.bytes[1],"]"))
print(paste0("physical.read.total.bytes[1]:[",physical.read.total.bytes[1],"]"))
print(paste0("physical.write.total.bytes[1]:[",physical.write.total.bytes[1],"]"))

# data.table 赋值 
sysstat[name=='redo size',bytes:= redo.bytes]
sysstat[name=='physical read bytes',bytes:= physical.read.bytes]
sysstat[name=='physical write bytes',bytes:= physical.write.bytes]
sysstat[name=='physical read total bytes',bytes:= physical.read.total.bytes]
sysstat[name=='physical write total bytes',bytes:= physical.write.total.bytes]


sysstat[,datetime := ymd_hms(datetime)]
head(sysstat)
