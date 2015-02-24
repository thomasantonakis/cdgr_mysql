# Load package
library(RMySQL)
library(xlsx)
library(plyr)
library(dplyr)
# Set timer
ptm <- proc.time()
# Establish connection
con <- dbConnect(RMySQL::MySQL(), host = '172.20.0.1', port = 3307, dbname = "beta",
                                  user = "tantonakis", password = "2secret4usAll!")
# Send query
rs <- dbSendQuery(con,"

SELECT `order_master`.`restaurant_id` ,
        `restaurant_detail`.`restaurant_name`,
        `order_master`.`user_id` ,
        FROM_UNIXTIME(`order_master`.`i_date`) as order_date,
        `order_master`.`status`, 
        round(`order_master`.`order_amt`, 2) as revenue,
        round(`order_master`.`order_commission`, 2) as commission ,
        `order_master`.`deliveryaddress_id` as address
        
        

FROM  `order_master` 
        JOIN `restaurant_detail`    USING (`restaurant_id`)


WHERE  `order_master`.`i_date` >= UNIX_TIMESTAMP('2015-2-1')
and ((`order_master`.`is_deleted` = 'N') and ((`order_master`.`status` = 'VERIFIED') or (`order_master`.`status` = 'REJECTED')))
and  `restaurant_detail`.`language_id` = 1


order by revenue DESC
                  

                  ")
# Fetch query results (n=-1) means all results
data <- dbFetch(rs, n=-1) 
# close connection
dbDisconnect(con)
# write results
# write.xlsx(x=data, file = 'tommys.xlsx', row.names = FALSE)
# Stop timer
proc.time() - ptm

########################
# Mazemeno
########################
con <- dbConnect(RMySQL::MySQL(), host = '172.20.0.1', port = 3307, dbname = "beta",
                 user = "tantonakis", password = "2secret4usAll!")
# Send query
rs <- dbSendQuery(con,"

SELECT `order_master`.`restaurant_id` ,
        `restaurant_detail`.`restaurant_name`,
        `order_master`.`user_id` ,
        FROM_UNIXTIME(`order_master`.`i_date`) as order_date,
        `order_master`.`status`, 
        round(sum(`order_master`.`order_amt`), 2) as revenue,
        round(sum(`order_master`.`order_commission`), 2) as commission ,
        COUNT(DISTINCT `order_master`.`restaurant_id`) AS distinct_rest,
        COUNT(DISTINCT `order_master`.`order_id`) AS distinct_ord,
        COUNT(DISTINCT `order_master`.`deliveryaddress_id`) AS distinct_add
        

FROM  `order_master` 
        JOIN `restaurant_detail`    USING (`restaurant_id`)


WHERE  `order_master`.`i_date` >= UNIX_TIMESTAMP('2015-2-1')
and ((`order_master`.`is_deleted` = 'N') and ((`order_master`.`status` = 'VERIFIED') or (`order_master`.`status` = 'REJECTED')))
and  `restaurant_detail`.`language_id` = 1

GROUP BY `order_master`.`user_id` 
order by revenue DESC
                  

                  ")
# Fetch query results (n=-1) means all results
peruser <- dbFetch(rs, n=-1) 
# close connection
dbDisconnect(con)

peruser$restaurant_id<-NULL
peruser$restaurant_name<-NULL
peruser$status<-NULL
peruser$order_date<-NULL

par(mfrow=c(3,1))
plot(table(peruser$distinct_rest), main="Distinct Restaurants February", type= 'b', lwd = 5, col = 'firebrick3', 
    xlab="Distinct Restaurants", ylab = "NUmber of Users")
plot(table(peruser$distinct_ord), main="Distinct Orders February", type= 'b', lwd = 5, col = 'deepskyblue4', 
     xlab="Distinct Orders", ylab = "NUmber of Users")
plot(table(peruser$distinct_add), main="Distinct Addresses February", type= 'b', lwd = 5, col = 'darkslateblue', 
     xlab="Distinct Addresses", ylab = "NUmber of Users")

# write.xlsx(x=peruser, file = 'peruser.xlsx', row.names = FALSE)
# Stop timer
proc.time() - ptm