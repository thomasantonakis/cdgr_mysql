# Load package
library(RMySQL)
# Set timer
ptm <- proc.time()
# Establish connection
con <- dbConnect(RMySQL::MySQL(), host = '172.20.0.1', port = 3307, dbname = "beta",
                 user = "tantonakis", password = "2secret4usAll!")
# Send query
rs <- dbSendQuery(con,"
                  
                  SELECT restaurant_master.restaurant_id as id, 
                        restaurant_master.restaurant_shortname, 
                        DATE(FROM_UNIXTIME(`order_master`.`i_date`)) as date, 
                        count(order_master.order_id) as orders, 
                        city_detail.city_name,
                        prefecture_detail.prefecture_name,
                        `user_master`.`last_name`

                FROM order_master
                JOIN restaurant_master
                ON (`restaurant_master`.`restaurant_id` = `order_master`.`restaurant_id` )
                LEFT JOIN `city_master`
                ON (`restaurant_master`.`restaurant_city_id` = `city_master`.`city_id`)
                LEFT JOIN `city_detail`
                ON (`city_detail`.`language_id` = 1 AND `city_master`.`city_id` = `city_detail`.`city_id`)
                LEFT JOIN `prefecture_detail`
                ON (`prefecture_detail`.`language_id` = 1 AND `city_master`.`prefecture_id` = `prefecture_detail`.`prefecture_id`)
                LEFT JOIN `user_master`
                ON (`city_master`.`city_account_manager_id` = `user_master`.`user_id`)

                WHERE ((`order_master`.`is_deleted` = 'N') AND ((`order_master`.`status` = 'VERIFIED') or (`order_master`.`status` = 'REJECTED')))
                AND `order_master`.`i_date` >= UNIX_TIMESTAMP('2015-05-01')
                AND `order_master`.`i_date` < UNIX_TIMESTAMP('2015-10-01')
                AND (restaurant_master.restaurant_id IN (1194, 38, 1081, 1332, 840, 2577 ,385, 2184, 336, 3263, 1196, 1273, 1859, 3223) 
                        OR restaurant_master.promoted = 1)

                GROUP BY id, date
                ORDER BY id, date
                 
                  
                  
                  
                  
                  ")
# Fetch query results (n=-1) means all results
promoted <- dbFetch(rs, n=-1) 
# close connection
dbDisconnect(con)
# Stop timer
proc.time() - ptm

write.csv2(x=promoted, file = "export.csv", row.names=FALSE)