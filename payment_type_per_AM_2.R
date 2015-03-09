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
                  
                  
                  SELECT month(FROM_UNIXTIME(`restaurant_balance`.`i_date`)) AS date,
                        `user_master`.`last_name` AS account_manager,
                        `balance_type` as type,
                        SUM(`restaurant_balance`.`balance_amount`) AS amount
                       
                        
                  
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
                  AND `restaurant_balance`.`i_date` < UNIX_TIMESTAMP('2015-03-01')

                    GROUP BY account_manager, type, date 
                  
                  
                  
                  ")
# Fetch query results (n=-1) means all results
invoices <- dbFetch(rs, n=-1) 
# close connection
dbDisconnect(con)
# Stop timer
proc.time() - ptm

report<-as.data.frame(table(invoices$account_manager, invoices$type, useNA="ifany"))
report<-report %>% spread(Var2, Freq)

# Set working directory
setwd("C:/Users/tantonakis/Google Drive/Scripts/AnalyticsProj/cdgr_mysql/Ad Hoc")
# Export
write.xlsx(x=report, file = "Payments_per_AM.xlsx")