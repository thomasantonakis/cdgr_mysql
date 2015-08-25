# # Load package
library(RMySQL)
# library(xlsx)
# # Set timer
ptm <- proc.time()
# Establish connection
con <- dbConnect(RMySQL::MySQL(), host = '172.20.0.1', port = 3307, dbname = "beta",
                 user = "tantonakis", password = "2secret4usAll!")
# Send query
rs <- dbSendQuery(con,"


SELECT count(*) AS users,
                `user_master`.`referal_source` AS SOURCE,
                YEAR(FROM_UNIXTIME(`user_master`.`verification_date`)) as year,
                MONTH(FROM_UNIXTIME(`user_master`.`verification_date`)) as month
FROM `user_master`

WHERE `user_master`.`verification_date` >= UNIX_TIMESTAMP('2015-07-01')
AND `user_master`.`verification_date` < UNIX_TIMESTAMP('2015-08-01')
AND `user_master`.`verification_date` - `user_master`.`i_date` <= 86400
AND `user_master`.`status` = 'VERIFIED'
AND `user_master`.`is_deleted` = 'N'

GROUP BY source, year, month

                  
                  ")
# Fetch query results (n=-1) means all results
active_users <- dbFetch(rs, n=-1) 
# close connection
dbDisconnect(con)
# Stop timer
proc.time() - ptm

write.csv2(x= active_users, file= "export.csv")