# Load package
library(RMySQL)
library(xlsx)
library(tidyr)
library(plyr)
library(dplyr)
# Set timer
ptm <- proc.time()
# Establish connection
con <- dbConnect(RMySQL::MySQL(), host = '172.20.0.1', port = 3307, dbname = "beta",
                 user = "tantonakis", password = "2secret4usAll!")
# Send query
rs <- dbSendQuery(con, "


                  SELECT   date(FROM_UNIXTIME(`order_master`.`i_date`)) as date,
                        `city_detail`.`city_name` as city,
                        count(distinct(`order_id`)) as orders
                  
                  FROM `order_master`
                LEFT JOIN `user_address`
        	ON (`order_master`.`deliveryaddress_id` = `user_address`.`address_id`)
		LEFT JOIN `city_master`
		ON (`user_address`.`city_id` = `city_master`.`city_id`)
		LEFT JOIN `city_detail`
		ON (`city_detail`.`language_id` = 1 AND `city_master`.`city_id` = `city_detail`.`city_id`)
                  
                  WHERE  ((`order_master`.`is_deleted` = 'N') and ((`order_master`.`status` = 'VERIFIED') or (`order_master`.`status` = 'REJECTED')))

                  AND `order_master`.`i_date` >= UNIX_TIMESTAMP('2015-03-01')
                  AND `city_detail`.`city_name` in ('Patra','Kalamata','Alexandroupoli','Serres','Xanthi','Komotini','Rethimno','Chania')
                  
                  
                  GROUP BY date , city
                  
                  
                  
                  
                  ")
# Fetch query results (n=-1) means all results
cities <- dbFetch(rs, n=-1) 
# close connection
dbDisconnect(con)
# Stop timer
proc.time() - ptm

# Set timer
ptm <- proc.time()
# Establish connection
con <- dbConnect(RMySQL::MySQL(), host = '172.20.0.1', port = 3307, dbname = "beta",
                 user = "tantonakis", password = "2secret4usAll!")
# Send query
rs <- dbSendQuery(con, "
                  
                  
                  SELECT   date(FROM_UNIXTIME(`order_master`.`i_date`)) as date,
                  `city_detail`.`city_name` as city,
                  `order_master`.`restaurant_id` as rest_id,
                  count(distinct(`order_id`)) as orders
                  
                  FROM `order_master`
                  LEFT JOIN `user_address`
                  ON (`order_master`.`deliveryaddress_id` = `user_address`.`address_id`)
                  LEFT JOIN `city_master`
                  ON (`user_address`.`city_id` = `city_master`.`city_id`)
                  LEFT JOIN `city_detail`
                  ON (`city_detail`.`language_id` = 1 AND `city_master`.`city_id` = `city_detail`.`city_id`)
                  
                  WHERE  ((`order_master`.`is_deleted` = 'N') and ((`order_master`.`status` = 'VERIFIED') or (`order_master`.`status` = 'REJECTED')))
                  AND `order_master`.`i_date` >= UNIX_TIMESTAMP('2015-03-01')
                  AND `city_detail`.`city_name` in ('Patra','Kalamata','Alexandroupoli','Serres','Xanthi','Komotini','Rethimno','Chania')
                AND `order_master`.`restaurant_id` in (1468,1469,1024,1679,1206,2220,1132,1130,1838,2221,1591,
                        1448,2399,2158,1026,2207,539,1570,2639,1181,1345,2268,1915,2553,1180,2146,914,1290,2573,2523,2339,2527,915)

                  
                  
                  GROUP BY date ,rest_id, city
                  
                  
                  
                  
                  ")
# Fetch query results (n=-1) means all results
restaurants <- dbFetch(rs, n=-1) 
# close connection
dbDisconnect(con)
# Stop timer
proc.time() - ptm

restaurants<- restaurants %>% spread(date, orders)
restaurants[is.na(restaurants)]<-0
cities<-cities %>% spread(date, orders)
write.xlsx(x=restaurants, file='export.xlsx')