# Load package
library(RMySQL)

# Set timer
ptm <- proc.time()


# Establish connection
con <- dbConnect(RMySQL::MySQL(), host = '172.20.0.1', port = 3307, dbname = "beta",
                 user = "tantonakis", password = "2secret4usAll!")
# Send query
rs <- dbSendQuery(con,"

select year(from_unixtime(`user_master`.`verification_date`)) AS `YEAR`,
                  week(from_unixtime(`user_master`.`verification_date`),0) AS `WEEK`,
                  floor((((abs((`order_master`.`i_date` - `user_master`.`verification_date`)) / 3600) / 24) / 7)) AS `WEEKS`,
                  
                  count(`order_master`.`order_id`) AS `ORDERS`,
                  round(sum(`order_master`.`order_amt`),2) AS `ORDERSUM`,
                  round(sum(`order_master`.`order_commission`),2) AS `COMMISSIONSUM` 
                  
                  from (`user_master` 
                  
                  join `order_master` 
                  
                  on((`user_master`.`user_id` = `order_master`.`user_id`))) 
                  
                  where ((`user_master`.`status` = 'VERIFIED') 
                  and (`user_master`.`referal_source` LIKE '%quizdom%')
                  and (`order_master`.`is_deleted` = 'N') 
                  and ((`order_master`.`status` = 'VERIFIED') 
                  or (`order_master`.`status` = 'REJECTED'))) 
                  
                  group by year(from_unixtime(`user_master`.`verification_date`)),
                  week(from_unixtime(`user_master`.`verification_date`),0),
                  floor((((abs((`order_master`.`i_date` - `user_master`.`verification_date`)) / 3600) / 24) / 7))
                  

                  
                                   ")


# Fetch query results (n=-1) means all results
cohort<- dbFetch(rs, n=-1) 
# close connection
dbDisconnect(con)
# Stop timer
proc.time() - ptm



# Establish connection
con <- dbConnect(RMySQL::MySQL(), host = '172.20.0.1', port = 3307, dbname = "beta",
                 user = "tantonakis", password = "2secret4usAll!")
# Send query
rs <- dbSendQuery(con,"

select year(from_unixtime(`user_master`.`verification_date`)) AS `YEAR`,
        week(from_unixtime(`user_master`.`verification_date`),0) AS `WEEK`,
        count(`user_master`.`user_id`) AS `USERS` 

from `user_master` 

where ((`user_master`.`is_deleted` = 'N') 
       and (`user_master`.`status` = 'VERIFIED'))
        and (`user_master`.`referal_source` LIKE '%quizdom%')

group by year(from_unixtime(`user_master`.`verification_date`)),
                week(from_unixtime(`user_master`.`verification_date`),0)

                  
                                   ")


# Fetch query results (n=-1) means all results
users<- dbFetch(rs, n=-1) 
# close connection
dbDisconnect(con)

# Stop timer
proc.time() - ptm

report<-merge(cohort, users)
report$channel<-"quizdom"
report<-report[c(1:3,7,4:6,8)]