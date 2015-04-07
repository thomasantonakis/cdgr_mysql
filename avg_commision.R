# Load package
library(RMySQL)
library(plyr)
library(xlsx)

# Set timer
ptm <- proc.time()

# Establish connection
con <- dbConnect(RMySQL::MySQL(), host = '172.20.0.1', port = 3307, dbname = "beta",
                 user = "tantonakis", password = "2secret4usAll!")
# Send query
rs <- dbSendQuery(con,"
                  

SELECT `restaurant_shortname` AS `RESTAURANT_SHORTNAME`,
                                YEAR(FROM_UNIXTIME(`order_master`.`i_date`)) AS YEAR, 
                                MONTH(FROM_UNIXTIME(`order_master`.`i_date`)) AS MONTH,
        			`restaurant_name` AS `RESTAURANT_NAME`,
				`restaurantgroup_id` AS `CHAIN_ID`,
				`city_name` AS `CITY_NAME`,
				count(`order_master`.`order_id`) AS `ORDERS`,
				ROUND(SUM(`order_master`.`order_amt`),2) AS `ORDERSUM`,
				ROUND(SUM(`order_master`.`order_commission`),2) AS `COMMISION`
				
				FROM `order_master`
				JOIN `restaurant_master`
				USING (`restaurant_id`)
				JOIN `restaurant_detail`
				ON (`restaurant_detail`.`restaurant_id` = `restaurant_master`.`restaurant_id` AND `restaurant_detail`.`language_id` = 1)
				LEFT JOIN `city_detail`
				ON (`restaurant_master`.`restaurant_city_id` = `city_detail`.`city_id` AND `city_detail`.`language_id` = 1)
				
                                where ((`order_master`.`is_deleted` = 'N') and ((`order_master`.`status` = 'VERIFIED') or (`order_master`.`status` = 'REJECTED')))
				AND `order_master`.`i_date` >= UNIX_TIMESTAMP('2014-12-01')
				AND `order_master`.`i_date` < UNIX_TIMESTAMP('2015-04-01')

				GROUP BY `order_master`.`restaurant_id`, month, year
				ORDER BY `restaurant_city_id` ASC, `restaurant_shortname` ASC
                  
                  "


)
# Fetch query results (n=-1) means all results
data <- dbFetch(rs, n=-1) 
# close connection
dbDisconnect(con)
# Stop timer
proc.time() - ptm



write.xlsx(x=data, file='commissions.xlsx')
