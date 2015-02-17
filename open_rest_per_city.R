# Load package
library(RMySQL)
# Set timer
ptm <- proc.time()
# Establish connection
con <- dbConnect(RMySQL::MySQL(), host = 'db.clickdelivery.gr', port = 3307, dbname = "beta",
                 user = "tantonakis", password = "2secret4usAll!")
# Send query
rs <- dbSendQuery(con, "


SELECT          `prefecture_detail`.`prefecture_name` AS PREFECTURE,
                COUNT(DISTINCT `restaurant_master`.`restaurant_id`) AS OPEN_RESTAURANTS

                FROM `restaurant_master`
                JOIN `city_master`
                ON (`restaurant_master`.`restaurant_city_id` = `city_master`.`city_id`)
                JOIN `city_detail`
                ON (`city_detail`.`language_id` = 1 AND `city_master`.`city_id` = `city_detail`.`city_id`)
                JOIN `prefecture_detail`
                ON (`prefecture_detail`.`language_id` = 1 AND `city_master`.`prefecture_id` = `prefecture_detail`.`prefecture_id`)

                WHERE `restaurant_master`.`start_date` < ('2015-02-16')
                AND `restaurant_id` != 19
                AND (`restaurant_master`.`signoff_date` IS NULL OR `restaurant_master`.`signoff_date` >= ('2015-02-16'))


                GROUP BY PREFECTURE 

                ORDER BY `prefecture_detail`.`prefecture_id`ASC, `restaurant_city_id` ASC

                  
                 ")
# Fetch query results (n=-1) means all results
company <- dbFetch(rs, n=-1) 
# close connection
dbDisconnect(con)
# Stop timer
proc.time() - ptm

# 
# SELECT          `prefecture_detail`.`prefecture_name` AS PREFECTURE,
# `city_detail`.`city_name` AS CITY,
# COUNT(DISTINCT `restaurant_master`.`restaurant_id`) AS OPEN_RESTAURANTS
# 
# FROM `restaurant_master`
# JOIN `city_master`
# ON (`restaurant_master`.`restaurant_city_id` = `city_master`.`city_id`)
# JOIN `city_detail`
# ON (`city_detail`.`language_id` = 1 AND `city_master`.`city_id` = `city_detail`.`city_id`)
# JOIN `prefecture_detail`
# ON (`prefecture_detail`.`language_id` = 1 AND `city_master`.`prefecture_id` = `prefecture_detail`.`prefecture_id`)
# 
# WHERE `restaurant_master`.`start_date` < ('2015-01-01')
# AND `restaurant_id` != 19
# AND (`restaurant_master`.`signoff_date` IS NULL OR `restaurant_master`.`signoff_date` >= ('2015-01-01'))
# 
# 
# GROUP BY PREFECTURE , CITY
# 
# ORDER BY `prefecture_detail`.`prefecture_id`ASC, `restaurant_city_id` ASC
# 
