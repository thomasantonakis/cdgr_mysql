# Load package
library(RMySQL)

# Set timer
ptm <- proc.time()
# Establish connection
con <- dbConnect(RMySQL::MySQL(), host = '172.20.0.1', port = 3307, dbname = "beta",
                 user = "tantonakis", password = "2secret4usAll!")
# Send query
rs <- dbSendQuery(con, "
                  

SELECT FROM_UNIXTIME(min(`order_master`.`i_date`)) as first_order_date, 
        YEAR(FROM_UNIXTIME(min(`order_master`.`i_date`))) as year, 
        month(FROM_UNIXTIME(min(`order_master`.`i_date`))) as month,
        `city_detail`.`city_name` AS CITY_NAME, 
        `prefecture_detail`.`prefecture_name` AS PREFECTURE, 
        `restaurant_master`.`restaurant_id` AS rest_id, 
        `user_master`.`last_name` AS ACCOUNT_MANAGER

FROM `order_master`

JOIN `restaurant_master`
        USING (`restaurant_id`)
LEFT JOIN `city_master`
        ON (`restaurant_master`.`restaurant_city_id` = `city_master`.`city_id`)
LEFT JOIN `city_detail`
        ON (`city_detail`.`language_id` =1 AND `city_master`.`city_id` = `city_detail`.`city_id`)
LEFT JOIN `prefecture_detail`
        ON (`prefecture_detail`.`language_id` = 1 AND `city_master`.`prefecture_id` = `prefecture_detail`.`prefecture_id`)
LEFT JOIN `user_master`
        ON (`city_master`.`city_account_manager_id` = `user_master`.`user_id`)

WHERE ((`order_master`.`is_deleted` = 'N') and ((`order_master`.`status` = 'VERIFIED') or (`order_master`.`status` = 'REJECTED')))
        AND `restaurant_master`.`country_id`=0
        AND `order_master`.`i_date` < UNIX_TIMESTAMP('2016-01-01')

GROUP BY rest_id

                  ")
# Fetch query results (n=-1) means all results
first_order <- dbFetch(rs, n=-1) 
# close connection
dbDisconnect(con)
# Stop timer
proc.time() - ptm


write.csv2(first_order, file = "export.csv",row.names=FALSE)
