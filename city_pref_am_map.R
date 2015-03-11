# Load package
library(RMySQL)
library(xlsx)
# Set timer
ptm <- proc.time()
# Establish connection
con <- dbConnect(RMySQL::MySQL(), host = '172.20.0.1', port = 3307, dbname = "beta",
                 user = "tantonakis", password = "2secret4usAll!")
# Send query
rs <- dbSendQuery(con, "
                  
                  
                  SELECT `city_detail`.`city_id`,
                        `city_detail`.`city_name` AS CITY_NAME,
                        `prefecture_detail`.`prefecture_name` AS PREFECTURE,
                        `user_master`.`last_name` AS ACCOUNT_MANAGER

                  FROM `city_master`
                  LEFT JOIN `city_detail`
        	  ON (`city_detail`.`language_id` = 1 AND `city_master`.`city_id` = `city_detail`.`city_id`)
	          LEFT JOIN `prefecture_detail`
		  ON (`prefecture_detail`.`language_id` = 1 AND `city_master`.`prefecture_id` = `prefecture_detail`.`prefecture_id`)
                  LEFT JOIN `user_master`
                  ON (`city_master`.`city_account_manager_id` = `user_master`.`user_id`)
                  
                   
                  
                  
                  
                  
                  
                  ")
# Fetch query results (n=-1) means all results
map <- dbFetch(rs, n=-1) 
# close connection
dbDisconnect(con)
# Stop timer
proc.time() - ptm

# Export
write.xlsx(x=map, file='map.xlsx')