library(data.table)
library(lubridate)
library(jiebaR)
library(wordcloud2)
library(stringr)
library(magrittr)
library(ROracle)


drv <- dbDriver("Oracle")
con <- dbConnect(drv, user = "apps", password = "apps", dbname = "10.0.5.84:1523/ETAP2")
sqlString <- "SELECT ledger_id,
  application_id,
  accounting_date,
  ACCOUNTING_CLASS_CODE,
  ACCOUNTED_CR,
  accounted_dr,
  DESCRIPTION,
  business_class_code
FROM xla_ae_lines
WHERE 1=1 and ledger_id = 2021
and accounting_date BETWEEN sysdate - 180 AND sysdate
and DESCRIPTION is not null"

rs <- dbSendQuery(con, sqlString)
xla_ae_lines <- fetch(rs, 100)

mixseg <- worker(stop_word='/root/Downloads/stop.txt')
seg_x <- function(x) {
	str_c(mixseg[x], collapse = " ")
}


split.desc <- sapply(xla_ae_lines$DESCRIPTION, seg_x, USE.NAMES=FALSE)

full.desc <- paste(split.desc, collapse = " ")
df <- freq(mixseg[full.desc])

df <- df[order(-df$freq),]
#df


#data.table(xla_ae_lines)[,desc.seg := split.desc][]
#ymd_hms(xla_ae_lines$accounting_date)
#xla_ae_lines$accounting_date
#names(xla_ae_lines)
#xla_ae_lines$ACCOUNTING_DATE
#ymd(xla_ae_lines$ACCOUNTING_DATE)
today()
#wordcloud2(df)
