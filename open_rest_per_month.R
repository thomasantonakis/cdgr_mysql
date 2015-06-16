# Load package
library(RMySQL)

# Set timer
ptm <- proc.time()

# Establish connection
con <- dbConnect(RMySQL::MySQL(), host = '172.20.0.1', port = 3307, dbname = "beta",
                 user = "tantonakis", password = "2secret4usAll!")
# Send query
rs <- dbSendQuery(con,"
                  

SELECT YEAR(`restaurant_master`.`start_date`) AS YEAR,
        month(`restaurant_master`.`start_date`) AS MONTH, 
        `city_detail`.`city_name` AS CITY_NAME, 
        `prefecture_detail`.`prefecture_name` AS PREFECTURE, 
        count(`restaurant_master`.`restaurant_id`) AS `open`,
        `user_master`.`last_name` AS ACCOUNT_MANAGER

FROM `restaurant_master`
       
LEFT JOIN `city_master`
        ON (`restaurant_master`.`restaurant_city_id` = `city_master`.`city_id`)
LEFT JOIN `city_detail`
        ON (`city_detail`.`language_id` = 1 AND `city_master`.`city_id` = `city_detail`.`city_id`)
LEFT JOIN `prefecture_detail`
        ON (`prefecture_detail`.`language_id` =1 AND `city_master`.`prefecture_id` = `prefecture_detail`.`prefecture_id`)
LEFT JOIN `user_master`
        ON (`city_master`.`city_account_manager_id` = `user_master`.`user_id`)

WHERE   `restaurant_master`.`start_date` >= '2014-01-01'
        AND `restaurant_master`.`start_date` < '2015-04-01'

GROUP BY year, MONTH, CITY_NAMe 

ORDER BY YEAR DESC, MONTH DESC, PREFECTURE ASC, CITY_NAME ASC 

                "


)
# Fetch query results (n=-1) means all results
data <- dbFetch(rs, n=-1) 
# close connection
dbDisconnect(con)
# Stop timer
proc.time() - ptm

# Write CSV in R
# write.csv(data, file = "export.csv",row.names=FALSE)