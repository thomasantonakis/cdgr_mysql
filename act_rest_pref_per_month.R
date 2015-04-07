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

SELECT         			
                                `prefecture_detail`.`prefecture_name` as PREF,
                                 COUNT(DISTINCT `restaurant_master`.`restaurant_id`) AS NUMBER_OF_RESTAURANTS,
				
				YEAR(FROM_UNIXTIME(`order_master`.`i_date`)) AS YEAR,
                                MONTH(FROM_UNIXTIME(`order_master`.`i_date`)) AS MONTH
				


				FROM `order_master`
				JOIN `restaurant_master`
				USING (`restaurant_id`)
				JOIN `restaurant_detail`
				ON (`restaurant_detail`.`restaurant_id` = `restaurant_master`.`restaurant_id` AND `restaurant_detail`.`language_id` = 1)
				LEFT JOIN `city_detail`
				ON (`restaurant_master`.`restaurant_city_id` = `city_detail`.`city_id` AND `city_detail`.`language_id` = 1)
                                LEFT JOIN `city_master`
                		ON ( `city_master`.`city_id` = `city_detail`.`city_id`)
                                LEFT JOIN `prefecture_detail`
        			ON (`prefecture_detail`.`language_id` = 1 AND `city_master`.`prefecture_id` = `prefecture_detail`.`prefecture_id`)

				WHERE ((`order_master`.`is_deleted` = 'N') and ((`order_master`.`status` = 'VERIFIED') or (`order_master`.`status` = 'REJECTED')))
				AND `order_master`.`i_date` >= UNIX_TIMESTAMP('2014-01-01')
				AND `order_master`.`i_date` < UNIX_TIMESTAMP('2015-04-01')


				GROUP BY  year, month, pref

				ORDER BY year ASC, month ASC, NUMBER_OF_RESTAURANTS DESC
                  

                  ")
# Fetch query results (n=-1) means all results
active_pref <- dbFetch(rs, n=-1) 
# close connection
dbDisconnect(con)
# Stop timer
proc.time() - ptm


