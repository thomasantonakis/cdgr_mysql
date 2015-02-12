# Load package
library(RMySQL)
# Set timer
ptm <- proc.time()
# Establish connection
con <- dbConnect(RMySQL::MySQL(), host = 'db.clickdelivery.gr', port = 3307, dbname = "beta",
                 user = "tantonakis", password = "2secret4usAll!")
# Send query
rs <- dbSendQuery(con,"

SELECT `mobile` 
        FROM  `sms_phones` 
WHERE  `batch` =7

                  ")
# Fetch query results (n=-1) means all results
received <- dbFetch(rs, n=-1) 
# close connection
dbDisconnect(con)
# Stop timer
proc.time() - ptm

#####################################
####### Verified ####################
#####################################

# Establish connection
con <- dbConnect(RMySQL::MySQL(), host = 'db.clickdelivery.gr', port = 3307, dbname = "beta",
                 user = "tantonakis", password = "2secret4usAll!")
# Send query
rs <- dbSendQuery(con,"

SELECT         `mobile`

                FROM `user_master`
        	

                  WHERE `user_master`.`verification_date` >= UNIX_TIMESTAMP('2015-02-04')
                  AND `user_master`.`is_deleted` = 'N'

                  GROUP BY `mobile`

                  ")
# Fetch query results (n=-1) means all results
verified <- dbFetch(rs, n=-1) 
# close connection
dbDisconnect(con)
# Stop timer
proc.time() - ptm

successes<-merge(received, verified)
max(dim(successes))
round(max(dim(successes))/max(dim(received))*100,2)