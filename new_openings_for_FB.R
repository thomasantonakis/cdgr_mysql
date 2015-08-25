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
        
        `restaurant_detail`.`restaurant_name`  as name,
        `prefecture_detail`.`prefecture_name` AS PREFECTURE,
        cuisine_detail.cuisine_name as primary_cuisine,
        `classification` AS CLASSIFICATION,
        `restaurant_master`.`restaurant_id` as country

FROM restaurant_master
INNER JOIN restaurant_cuisine AS rest_cuisine_out
USING(restaurant_id)
INNER JOIN cuisine_detail
USING(cuisine_id)
LEFT JOIN `restaurant_detail` ON ( `restaurant_master`.`restaurant_id` = `restaurant_detail`.`restaurant_id` 
AND `restaurant_detail`.`language_id` =1 ) 
LEFT JOIN `city_master` ON ( `restaurant_master`.`restaurant_city_id` = `city_master`.`city_id` ) 
LEFT JOIN `city_detail` ON ( `city_detail`.`language_id` =1
AND `city_master`.`city_id` = `city_detail`.`city_id` ) 
LEFT JOIN `prefecture_detail` ON ( `prefecture_detail`.`language_id` =1
AND `city_master`.`prefecture_id` = `prefecture_detail`.`prefecture_id` ) 
LEFT JOIN `user_master` ON ( `city_master`.`city_account_manager_id` = `user_master`.`user_id` ) 
WHERE 
rest_cuisine_out.display_order = (
SELECT MIN(rest_cuisine_in.display_order)
FROM restaurant_cuisine AS rest_cuisine_in
WHERE rest_cuisine_in.restaurant_id = rest_cuisine_out.restaurant_id 
)
AND cuisine_detail.language_id = 1
AND `restaurant_master`.`start_date` >= '2015-06-01'
AND `restaurant_master`.`restaurant_id` != 19
AND (`restaurant_master`.`signoff_date` IS NULL )
        

group by name, primary_cuisine
order by country asc, PREFECTURE asc , primary_cuisine asc ,name desc


                  ")
# Fetch query results (n=-1) means all results
rest <- dbFetch(rs, n=-1) 
# close connection
dbDisconnect(con)
# Stop timer
proc.time() - ptm


