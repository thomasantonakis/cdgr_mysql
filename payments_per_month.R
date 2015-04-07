# Load package
library(RMySQL)
library(xlsx)
library(plyr)
library(tidyr)
library(dplyr)
# Set timer
ptm <- proc.time()
# Establish connection
con <- dbConnect(RMySQL::MySQL(), host = '172.20.0.1', port = 3307, dbname = "beta",
                 user = "tantonakis", password = "2secret4usAll!")
# Send query
rs <- dbSendQuery(con, "
                  
                  
                  SELECT YEAR(FROM_UNIXTIME(`restaurant_balance`.`i_date`)) as year, 
                        MONTH(FROM_UNIXTIME(`restaurant_balance`.`i_date`)) as month,
                        `balance_type` as type,
                        `user_master`.`last_name` as user_id,
                        -sum(`restaurant_balance`.`balance_amount`) AS amount,
                        count(distinct(`restaurant_balance`.`restaurantbalance_id`)) as transaction_count
                        
                        
                  
                  FROM `restaurant_balance`
                              JOIN `restaurant_master`
                		USING (`restaurant_id`)
				LEFT JOIN `city_master`
				ON (`restaurant_master`.`restaurant_city_id` = `city_master`.`city_id`)
				LEFT JOIN `city_detail`
				ON (`city_detail`.`language_id` = 1 AND `city_master`.`city_id` = `city_detail`.`city_id`)
				LEFT JOIN `prefecture_detail`
				ON (`prefecture_detail`.`language_id` = 1 AND `city_master`.`prefecture_id` = `prefecture_detail`.`prefecture_id`)
				LEFT JOIN `user_master`
				ON (`city_master`.`city_account_manager_id` = `user_master`.`user_id`)
                  
                  WHERE `restaurant_balance`.`i_date` >= UNIX_TIMESTAMP('2015-01-01')
                  AND `restaurant_balance`.`i_date` < UNIX_TIMESTAMP('2015-04-01')
                  AND  `balance_type` IN ('CASH PAYMENT',  'EUROBANK PAYMENT',  'PIRAEUS PAYMENT', 'SETOFF')

                  GROUP BY year ,month,user_id, type
                  ORDER BY year ,month,user_id, type
                  
                  
                  ")
# Fetch query results (n=-1) means all results
invoices_2 <- dbFetch(rs, n=-1) 
# close connection
dbDisconnect(con)
# Stop timer
proc.time() - ptm



# Set working directory
setwd("C:/Users/tantonakis/Google Drive/Scripts/AnalyticsProj/cdgr_mysql/Ad Hoc")
# Export
write.xlsx(x=invoices_2, file = "Payments_per_AM_type_month.xlsx")