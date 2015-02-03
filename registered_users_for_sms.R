# Load package
library(RMySQL)
# Set timer
ptm <- proc.time()
# Establish connection
con <- dbConnect(RMySQL::MySQL(), host = 'db.clickdelivery.gr', port = 3307, dbname = "beta",
                 user = "tantonakis", password = "2secret4usAll!")
# Send query
rs <- dbSendQuery(con,"

SELECT `user_id`, `mobile`
FROM  `user_master` 
WHERE `status` =  'NEW'
AND  `usertype_id` =100
AND  `is_active` =  'Y'
AND  `is_deleted` =  'N'
AND  `mobile` LIKE '69%'
AND  `sms` = 1
AND  `i_date` >= UNIX_TIMESTAMP(  '2014-10-01' ) 

                  ")
# Fetch query results (n=-1) means all results
d1 <- dbFetch(rs, n=-1) 
# close connection
dbDisconnect(con)
# Stop timer
proc.time() - ptm


library(xlsx)
write.csv(x = d1, file = "sms_4_2_2015.csv",
            row.names = FALSE)