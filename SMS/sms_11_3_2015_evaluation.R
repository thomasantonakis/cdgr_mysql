# Load package
library(RMySQL)
library(xlsx)

# Set timer
ptm <- proc.time()

# Read new data every day or week and incorporate to orders

# Establish connection
con <- dbConnect(RMySQL::MySQL(), host = '172.20.0.1', port = 3307, dbname = "beta",
                 user = "tantonakis", password = "2secret4usAll!")
# Send query
rs <- dbSendQuery(con,"
                  
                  SELECT `order_master`.`user_id` ,
                         `user_master`.`mobile`
                  
                  FROM  `order_master` 
                  JOIN `user_master`
                  ON `order_master`.`user_id` = `user_master`.`user_id`
                  
                  WHERE  `order_master`.`i_date` >= UNIX_TIMESTAMP('2015-03-12')
                  and    `order_master`.`i_date` <= UNIX_TIMESTAMP('2015-03-16')
                  and ((`order_master`.`is_deleted` = 'N') and ((`order_master`.`status` = 'VERIFIED') or (`order_master`.`status` = 'REJECTED')))
                  
                  
                  ")
# Fetch query results (n=-1) means all results
new_sms <- dbFetch(rs, n=-1) 
# close connection
dbDisconnect(con)
# Stop timer
proc.time() - ptm

setwd("C:/Users/tantonakis/Google Drive/Scripts/AnalyticsProj/cdgr_mysql/SMS")
received<-read.csv("sms_11_3_2015_v2.csv", sep=";", stringsAsFactors=FALSE)
common<-merge(new_sms, received)
common_unique<-unique(common)
round(max(dim(common_unique))/max(dim(received))*100, 2)

# write.xlsx(x=common, file="sms_11_3.xlsx")