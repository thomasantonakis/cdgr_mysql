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
                  and (`user_master`.`referal_source` = 'google|cpc|brand')
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
        and (`user_master`.`referal_source` = 'google|cpc|brand')

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

##############################################
######## b) Organic ##########################
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
                  and (`user_master`.`referal_source` LIKE  '%google.%'
                        or `user_master`.`referal_source` LIKE  '%search%'
                        or `user_master`.`referal_source` LIKE  '%bing%')
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
                  and (`user_master`.`referal_source` LIKE  '%google.%'
                        or `user_master`.`referal_source` LIKE  '%search%'
                        or `user_master`.`referal_source` LIKE  '%bing%')
                  
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
report$channel<-"organic"
final<-rbind(final, report)

##############################################
######## c) Direct ###########################
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
                  and (`user_master`.`referal_source` = '')
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
                  and (`user_master`.`referal_source` = '')
                  
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
report$channel<-"direct"
final<-rbind(final, report)


##############################################
######## d) All without Brand ################
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
                  and `referal_source` LIKE  'google|cpc%'
                  AND  `referal_source` NOT LIKE  'google|cpc|brand'
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
        and `referal_source` LIKE  'google|cpc%'
        AND  `referal_source` NOT LIKE  'google|cpc|brand'
        

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
report$channel<-'withoutbrand'
final<-rbind(final, report)


##############################################
######## E) Goody's ##########################
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
                  and `referal_source` LIKE  'google|cpc|Ch.Goodys%'
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
        and `referal_source` LIKE  'google|cpc|Ch.Goodys%'

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
report$channel<-'goodys'
final<-rbind(final, report)

##############################################
######## f) Generic ##########################
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
                  and `referal_source` LIKE  'google|cpc|Generic'
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
                  and `referal_source` LIKE  'google|cpc|Generic'
                  
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
report$channel<-'generic'
final<-rbind(final, report)



##############################################
######## g) General_FO #######################
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
                  and `referal_source` LIKE  'google|cpc|General_FO%'
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
                  and `referal_source` LIKE  'google|cpc|General_FO%'
                  
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
report$channel<-'general_fo'
final<-rbind(final, report)



##############################################
######## h) Deliveras ########################
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
                  and `referal_source` LIKE  'google|cpc|Competitors|Deliveras.gr'
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
                  and `referal_source` LIKE  'google|cpc|Competitors|Deliveras.gr'
                  
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
report$channel<-'deliveras'
final<-rbind(final, report)


##############################################
######## i) Deliveras ########################
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
                  and `referal_source` LIKE  'google|cpc|Competitors|Efood.gr'
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
                  and `referal_source` LIKE  'google|cpc|Competitors|Efood.gr'
                  
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
report$channel<-'efood'
final<-rbind(final, report)

##############################################
######## j) Android ##########################
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
                  and `referal_source` LIKE  'Android%'
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
                  and `referal_source` LIKE  'Android%'
                  
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
report$channel<-'android'
final<-rbind(final, report)

##############################################
######## k) IOS ##############################
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
                  and `referal_source` LIKE  'IOS%'
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
                  and `referal_source` LIKE  'IOS%'
                  
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
report$channel<-'ios'
final<-rbind(final, report)

##############################################
######## l) Coupons ##########################
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
                  
                  on(`user_master`.`user_id` = `order_master`.`user_id`)
                      JOIN user_promos
                      ON (`user_promos`.`user_id` != 0 AND `user_master`.`user_id` = `user_promos`.`user_id`) )
                  
                  where ((`user_master`.`status` = 'VERIFIED') 
                  and user_promos.promo_id RLIKE '^C'
                  and user_master.verification_date >= user_promos.valid_after
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
                  SUM(IF((user_promos.promo_id RLIKE '^C' AND user_master.verification_date >= user_promos.valid_after AND user_master.status = 'VERIFIED'), 1, 0)) AS USERS
                  
                  from `user_master` 
                      JOIN user_promos
                      ON (`user_promos`.`user_id` != 0 AND `user_master`.`user_id` = `user_promos`.`user_id` )
                  
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
report$channel<-'coupons'
final<-rbind(final, report)


write.csv2(x=final, file = "export.csv", row.names=FALSE)

library(RGA)
client.id = '543269518849-dcdk7eio32jm2i4hf241mpbdepmifj00.apps.googleusercontent.com'
client.secret = '9wSw6gyDVXtcgqEe0XazoBWG'

ga_token<-authorize(client.id, client.secret, cache = getOption("rga.cache"))

accs<-list_profiles(account.id = "~all", webproperty.id = "~all",
                    start.index = NULL, max.results = NULL, ga_token)

accounts<-data.frame(id = accs$id)
accounts$desc<-c('website', 'android', 'ios', 'youtube')
rm(accs)


startdate = as.Date('2013-1-1')
enddate = as.Date('2015-5-31')

adwords<-get_ga(25764841, start.date = startdate, end.date = enddate,
                
                metrics = "
                        ga:adCost
                ",
                
                dimensions = "
                        ga:year,
                        ga:month,
                        ga:campaign,
                        ga:adgroup
                ",
                sort = NULL, 
                filters = NULL,
                segment = NULL, 
                sampling.level = NULL,
                start.index = NULL, 
                max.results = NULL, 
                ga_token
)

adwords$cat<-1
adwords$cat[(grepl("Android", adwords$campaign))]<-"android"
adwords$cat[(grepl("iOS", adwords$campaign))]<-"iOS"
adwords$cat[(grepl("General_FO", adwords$campaign))]<-"General_FO"
adwords$cat[(grepl("Generic", adwords$campaign))]<-"Generic"
adwords$cat[adwords$campaign=="Generic"]<-"Generic"
adwords$cat[(grepl("Ch.Goodys", adwords$campaign))]<-"Goodys"
adwords$cat[(grepl("Brand", adwords$campaign))]<-"Brand"
adwords$cat[(grepl("DSP", adwords$campaign,ignore.case = TRUE))]<-0
adwords$cat[(grepl("TV", adwords$campaign))]<-0
adwords$cat[(grepl("mund", adwords$campaign,ignore.case = TRUE))]<-0
adwords$cat[(grepl("paratasi", adwords$campaign))]<-0
adwords$cat[(grepl("1", adwords$campaign,ignore.case = TRUE))]<-0
adwords$cat[(grepl("efood", adwords$adgroup,ignore.case = TRUE))]<-"efood"
adwords$cat[(grepl("e-food", adwords$adgroup,ignore.case = TRUE))]<-"efood"
adwords$cat[(grepl("deliveras", adwords$adgroup,ignore.case = TRUE))]<-"deliveras"
adwords$cat[(grepl("Remarketing", adwords$campaign,ignore.case = TRUE))]<-"remarketing"
adwords$cat[adwords$cat=="1"]<-"withoutbrand"

write.csv2(x=adwords, file = "export.csv", row.names=FALSE)