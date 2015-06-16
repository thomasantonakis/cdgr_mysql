# Load package
library(RMySQL)

# Set timer
ptm <- proc.time()


# Establish connection
con <- dbConnect(RMySQL::MySQL(), host = '172.20.0.1', port = 3307, dbname = "beta",
                 user = "tantonakis", password = "2secret4usAll!")
# Send query
rs <- dbSendQuery(con,"

SELECT YEAR(FROM_UNIXTIME(`user_master`.`verification_date`)) as YEAR, MONTH(FROM_UNIXTIME(`user_master`.`verification_date`)) as MONTH,
FLOOR(ABS((`order_master`.`i_date` - `user_master`.`verification_date` )) /3600 /24 /30) AS MONTHS,
COUNT(DISTINCT `user_id`) AS USERS
FROM `user_master`
JOIN `order_master`
USING (`user_id`)
WHERE `user_master`.`status` = 'VERIFIED'
AND `order_master`.`is_deleted` = 'N'
AND ABS((`order_master`.`i_date` - `user_master`.`verification_date`) > 60 * 60)
GROUP BY YEAR, MONTH, MONTHS


                  
                                   ")


# Fetch query results (n=-1) means all results
users_coh<- dbFetch(rs, n=-1) 
# close connection
dbDisconnect(con)
# Stop timer
proc.time() - ptm



write.csv2(x=users_coh, file = "export.csv", row.names=FALSE)