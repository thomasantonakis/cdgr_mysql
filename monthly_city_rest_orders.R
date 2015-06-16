# Load package
library(RMySQL)

# Set timer
ptm <- proc.time()

# Establish connection
con <- dbConnect(RMySQL::MySQL(), host = '172.20.0.1', port = 3307, dbname = "beta",
                 user = "tantonakis", password = "2secret4usAll!")
# Send query
rs <- dbSendQuery(con,"

SELECT YEAR(FROM_UNIXTIME(`order_master`.`i_date`)) as year,
MONTH(FROM_UNIXTIME(`order_master`.`i_date`)) as month,
`restaurant_shortname` AS `RESTAURANT_SHORTNAME`,
`restaurant_name` AS `RESTAURANT_NAME`,
`restaurant_master`.`browser` as browser,
`restaurantgroup_id` AS `CHAIN_ID`,
`city_name` AS `CITY_NAME`,
`prefecture_detail`.`prefecture_name`,
count(`order_master`.`order_id`) AS `ORDERS`,
ROUND(SUM(`order_master`.`order_amt`),2) AS `ORDERSUM`,
ROUND(SUM(`order_master`.`order_commission`),2) AS `COMM`

FROM `order_master`
JOIN `restaurant_master`
USING (`restaurant_id`)
JOIN `restaurant_detail`
ON (`restaurant_detail`.`restaurant_id` = `restaurant_master`.`restaurant_id` AND `restaurant_detail`.`language_id` = 1)
LEFT JOIN `city_detail`
ON (`restaurant_master`.`restaurant_city_id` = `city_detail`.`city_id` AND `city_detail`.`language_id` =1)
LEFT JOIN `city_master`
ON (`restaurant_master`.`restaurant_city_id` = `city_master`.`city_id`)
LEFT JOIN `prefecture_detail`
ON (`prefecture_detail`.`language_id` =1 AND `city_master`.`prefecture_id` = `prefecture_detail`.`prefecture_id`)

WHERE ((`order_master`.`is_deleted` = 'N') AND ((`order_master`.`status` = 'VERIFIED') or (`order_master`.`status` = 'REJECTED')))
AND `order_master`.`i_date` >= UNIX_TIMESTAMP('2014-01-01')
AND `order_master`.`i_date` < UNIX_TIMESTAMP('2015-06-01')

GROUP BY year, month, `order_master`.`restaurant_id`
ORDER BY year ASC, month ASC,`restaurant_city_id` ASC, `restaurant_shortname` ASC

                  
                  ")


# Fetch query results (n=-1) means all results
TOM<- dbFetch(rs, n=-1) 
# close connection
dbDisconnect(con)
# Stop timer
proc.time() - ptm


write.csv2(x=TOM, file = "export.csv", row.names=FALSE)