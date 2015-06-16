# Load package
library(RMySQL)

# Set timer
ptm <- proc.time()

##############################################
######## a) Brand ############################
##############################################

# Establish connection
con <- dbConnect(RMySQL::MySQL(), host = '172.20.0.1', port = 3307, dbname = "beta",
                 user = "tantonakis", password = "2secret4usAll!")
# Send query
rs <- dbSendQuery(con,"

select year(from_unixtime(`user_master`.`verification_date`)) AS `YEAR`,
                  month(from_unixtime(`user_master`.`verification_date`)) AS `MONTH`,
                  floor((((abs((`order_master`.`i_date` - `user_master`.`verification_date`)) / 3600) / 24) / 30.416)) AS `MONTHS`,
                  
                  count(`order_master`.`order_id`) AS `ORDERS`,
                  sum(`order_master`.`order_amt`) AS `ORDERSUM`,
                  sum(`order_master`.`order_commission`) AS `COMMISSIONSUM` 
                  
                  from (`user_master` 
                  
                  join `order_master` 
                  
                  on((`user_master`.`user_id` = `order_master`.`user_id`))) 
                  
                  where ((`user_master`.`status` = 'VERIFIED') 
                  and (`order_master`.`is_deleted` = 'N') 
                  and ((`order_master`.`status` = 'VERIFIED') 
                  or (`order_master`.`status` = 'REJECTED'))) 
                  
                  group by year(from_unixtime(`user_master`.`verification_date`)),
                  month(from_unixtime(`user_master`.`verification_date`)),
                  floor((((abs((`order_master`.`i_date` - `user_master`.`verification_date`)) / 3600) / 24) / 30.416))
                  

                  
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
        month(from_unixtime(`user_master`.`verification_date`)) AS `MONTH`,
        count(`user_master`.`user_id`) AS `USERS` 

from `user_master` 

where ((`user_master`.`is_deleted` = 'N') 
       and (`user_master`.`status` = 'VERIFIED'))
       

group by year(from_unixtime(`user_master`.`verification_date`)),
        month(from_unixtime(`user_master`.`verification_date`))

                  
                                   ")


# Fetch query results (n=-1) means all results
users<- dbFetch(rs, n=-1) 
# close connection
dbDisconnect(con)

# Stop timer
proc.time() - ptm

report<-merge(cohort, users)
report$channel<-"brand"
final<-report



write.csv2(x=final, file = "export.csv", row.names=FALSE)