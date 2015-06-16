### Appsflyer in-app events report
# Set timer
ptm <- proc.time()
# construct the API call
a<- "https://hq.appsflyer.com/export/id819241242/in_app_events_report?api_token=2c300c9a-ce57-4072-b4bd-537df539fb80&from="
b<-"&to="
# SOS ! CHANGE DATES
# Note to self: In order to report from 01/01/2015 to 31/01/2015 
# we need to call API from one day earlier to one day later 31/12/2014 - 01/02/2015
startdate<- '2015-05-22'
enddate<- '2015-05-28'
# paste the API call URL to a string
fileUrl<-paste(a,startdate,b,enddate,sep="")
# Make the call to the API and save it locally
download.file(fileUrl, destfile="./appsflyer/events.csv", method="auto")
# Read from CSV
data<-read.csv("./appsflyer/events.csv", stringsAsFactors = FALSE)
# Keep only the columns we need
data<-data[,c(1:4,8)]
# Adjust for the time zone
data$Click.Time<-as.Date(as.POSIXct(data$Click.Time,tz="EET")+3*60*60) # adding 3hours 
data$Install.Time<-as.Date(as.POSIXct(data$Install.Time,tz="EET")+3*60*60) # adding 3hours 
data$Event.Time<-as.Date(as.POSIXct(data$Event.Time,tz="EET")+3*60*60) # adding 3hours 
# Data frame is ready to be explored
# table rows, columns, layers
# table(data$Event.Time, data$Media.Source..pid., data$Event.Name)
af_reg<-as.data.frame(table(data$Event.Time, data$Media.Source..pid., data$Event.Name)[,,2])
af_ver<-as.data.frame(table(data$Event.Time, data$Media.Source..pid., data$Event.Name)[,,6])
af_ord<-as.data.frame(table(data$Event.Time, data$Media.Source..pid., data$Event.Name)[,,3])
# Drop row.names, convert to a recular column, rearrange columns
af_reg$date = rownames(af_reg)
rownames(af_reg) = NULL
af_reg<-af_reg[,c(3,1,2)]
af_ver$date = rownames(af_ver)
rownames(af_ver) = NULL
af_ver<-af_ver[,c(3,1,2)]
af_ord$date = rownames(af_ord)
rownames(af_ord) = NULL
af_ord<-af_ord[,c(3,1,2)]

# Database Data per day

################################################
# Registrations
################################################

# Load package
library(RMySQL)
# Open VPN if not in the office
# Establish connection
con <- dbConnect(RMySQL::MySQL(), host = '172.20.0.1', port = 3307, dbname = "beta",
                 user = "tantonakis", password = "2secret4usAll!")
# Send query
rs <- dbSendQuery(con,"
                  
                  SELECT DATE(FROM_UNIXTIME(`user_master`.`i_date`))AS DATE,
                  
                  count(distinct`user_master`.`user_id`) as users,
                  `user_master`.`referal_source` as source
                  
                  
                  FROM `user_master`
                  
                  WHERE `user_master`.`i_date` >= UNIX_TIMESTAMP('2015-05-22')
                  AND `user_master`.`i_date` < UNIX_TIMESTAMP('2015-06-01')
                  AND `user_master`.`is_deleted` = 'N'
                  AND (`user_master`.`referal_source`  LIKE '%IOS|quizdom%'
                        OR `user_master`.`referal_source`  LIKE '%IOS|facebook%')
                  
                  GROUP BY DATE, source
                  ORDER BY DATE, source
                  
                  ")
# Fetch query results (n=-1) means all results
db_reg <- dbFetch(rs, n=-1) 
# close connection
dbDisconnect(con)
# Stop timer
proc.time() - ptm
db_reg$source[grepl("^IOS\\|facebook", x = db_reg$source)]<-"Facebook Ads"
db_reg$source[grepl("^IOS\\|quizdom", x = db_reg$source)]<-"quizdom"

library(plyr)
library(dplyr)
library(tidyr)
db_reg<-ddply(db_reg,.(DATE,source),summarize,users = sum(users))
db_reg<-db_reg %>% spread(source, users)

################################################
# Verifications
################################################

# Load package
# library(RMySQL)
# Open VPN if not in the office
# Establish connection
con <- dbConnect(RMySQL::MySQL(), host = '172.20.0.1', port = 3307, dbname = "beta",
                 user = "tantonakis", password = "2secret4usAll!")
# Send query
rs <- dbSendQuery(con,"
                  
                  SELECT DATE(FROM_UNIXTIME(`user_master`.`i_date`))AS DATE,
                  
                  count(distinct`user_master`.`user_id`) as users,
                  `user_master`.`referal_source` as source
                  
                  
                  FROM `user_master`
                  
                  WHERE `user_master`.`verification_date` >= UNIX_TIMESTAMP('2015-05-22')
                  AND `user_master`.`verification_date` < UNIX_TIMESTAMP('2015-06-01')
                  AND `user_master`.`status` = 'VERIFIED'
                  AND `user_master`.`is_deleted` = 'N'
                  AND (`user_master`.`referal_source`  LIKE '%IOS|quizdom%'
                        OR `user_master`.`referal_source`  LIKE '%IOS|facebook%')
                  
                  GROUP BY DATE, source
                  ORDER BY DATE, source
                  
                  ")
# Fetch query results (n=-1) means all results
db_ver <- dbFetch(rs, n=-1) 
# close connection
dbDisconnect(con)
# Stop timer
proc.time() - ptm
db_ver$source[grepl("^IOS\\|facebook", x = db_ver$source)]<-"Facebook Ads"
db_ver$source[grepl("^IOS\\|quizdom", x = db_ver$source)]<-"quizdom"

# 
# library(plyr)
# library(dplyr)
# library(tidyr)
db_ver<-ddply(db_ver,.(DATE,source),summarize,users = sum(users))
db_ver<-db_ver %>% spread(source, users)


################################################
# Orders
################################################

# Load package
# library(RMySQL)
# Open VPN if not in the office
# Establish connection
con <- dbConnect(RMySQL::MySQL(), host = '172.20.0.1', port = 3307, dbname = "beta",
                 user = "tantonakis", password = "2secret4usAll!")
# Send query
rs <- dbSendQuery(con,"
                  
                  SELECT DATE(FROM_UNIXTIME(`order_master`.`i_date`))AS DATE,
                  
                  count(distinct`order_master`.`user_id`) as users,
                  `order_master`.`order_referal` as source
                  
                  FROM `order_master`

                  WHERE `order_master`.`i_date` >= UNIX_TIMESTAMP('2015-01-01')
                  AND `order_master`.`i_date` < UNIX_TIMESTAMP('2015-06-01')
                  AND `order_master`.`status` IN ('VERIFIED', 'REJECTED')
                  AND `order_master`.`is_deleted` = 'N'
                  AND (`order_master`.`order_referal`  LIKE '%IOS|quizdom%'
                        OR `order_master`.`order_referal`  LIKE '%IOS|facebook%')
                  
                  GROUP BY DATE, source
                  ORDER BY DATE, source
                  
                  ")
# Fetch query results (n=-1) means all results
db_ord <- dbFetch(rs, n=-1) 
# close connection
dbDisconnect(con)
# Stop timer
proc.time() - ptm
db_ord$source[grepl("^IOS\\|facebook", x = db_ord$source)]<-"Facebook Ads"
db_ord$source[grepl("^IOS\\|quizdom", x = db_ord$source)]<-"quizdom"

# 
# library(plyr)
# library(dplyr)
# library(tidyr)
db_ord<-ddply(db_ord,.(DATE,source),summarize,users = sum(users))
db_ord<-db_ord %>% spread(source, users)

db_reg[is.na(db_reg)]<-0
db_ver[is.na(db_ver)]<-0
db_ord[is.na(db_ord)]<-0
