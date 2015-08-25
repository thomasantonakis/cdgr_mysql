# Load package
library(RMySQL)

# Establish connection
con <- dbConnect(RMySQL::MySQL(), host = '172.20.0.1', port = 3307, dbname = "beta",
                 user = "tantonakis", password = "2secret4usAll!")
# Send query
rs <- dbSendQuery(con,"
                  
                  SELECT count( distinct `order_master`.`order_id`) as orders,  
                  Date(from_unixtime(`order_master`.`i_date`)) as date,
                  `prefecture_detail`.`prefecture_name` as pref
                  
                  FROM  `order_master`
                  JOIN `restaurant_master`
                  ON (`order_master`.`restaurant_id` = `restaurant_master`.`restaurant_id`)
	  	  JOIN `city_master`
	  	  ON (`restaurant_master`.`restaurant_city_id` = `city_master`.`city_id`)
		  JOIN `city_detail`
		  ON (`city_detail`.`language_id` = 1 AND `city_master`.`city_id` = `city_detail`.`city_id`)
		  JOIN `prefecture_detail`
		  ON (`prefecture_detail`.`language_id` = 1 AND `city_master`.`prefecture_id` = `prefecture_detail`.`prefecture_id`)
                  
                  WHERE `order_master`.`i_date` >= UNIX_TIMESTAMP('2015-05-01')
                  AND `order_master`.`i_date` < UNIX_TIMESTAMP('2015-09-01')
                  AND ((`order_master`.`is_deleted` = 'N') and ((`order_master`.`status` = 'VERIFIED') or (`order_master`.`status` = 'REJECTED')))
                  AND `order_master`.`country_id` = 1

                  Group by date, pref
                  
                  
                  ")
# Fetch query results (n=-1) means all results
orders_cyprus <- dbFetch(rs, n=-1) 
# close connection
dbDisconnect(con)
