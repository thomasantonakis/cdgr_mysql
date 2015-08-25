# Load package
library(RMySQL)

# Set timer
ptm <- proc.time()


# Establish connection
con <- dbConnect(RMySQL::MySQL(), host = '172.20.0.1', port = 3307, dbname = "beta",
                 user = "tantonakis", password = "2secret4usAll!")
# Send query
rs <- dbSendQuery(con,"
                  
                  SELECT MONTH(FROM_UNIXTIME(`order_master`.`i_date`)) AS date,
                        `user_payment_method`.`method_type` as type,
                        `user_master`.`last_name` AS account_manager,
                        `prefecture_detail`.`prefecture_name` as pref,
                        `city_detail`.`city_name` as city,
                        count(`order_master`.`order_id`) as orders,
                        ROUND(SUM(`order_master`.`order_amt`),2) as revenue,
                        ROUND(SUM(`order_master`.`order_commission`),2) as commission
                
                FROM `order_master`
                LEFT JOIN `prepayment_transaction`
                ON (`order_master`.`prepayment_transaction_id` = `prepayment_transaction`.`prepayment_transaction_id`)
                LEFT JOIN `user_payment_method` 
                ON (`prepayment_transaction`.`payment_method_id`=`user_payment_method`.`user_payment_method_id`)
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

                WHERE `order_master`.`i_date` >= UNIX_TIMESTAMP('2015-04-24')
                  AND `order_master`.`i_date` < UNIX_TIMESTAMP('2015-06-22')
                AND ((`order_master`.`is_deleted` = 'N') 
                and ((`order_master`.`status` = 'VERIFIED') 
                        or (`order_master`.`status` = 'REJECTED')))
                AND `order_master`.`prepayment_transaction_id` IS NOT NULL

               GROUP BY date, type,account_manager, pref,city
                

                UNION  

               SELECT MONTH(FROM_UNIXTIME(`order_master`.`i_date`)) AS date,
                        'orders' as type,
                  `user_master`.`last_name` AS account_manager,
                  `prefecture_detail`.`prefecture_name` as pref,
                  `city_detail`.`city_name` as city,
                  count(`order_master`.`order_id`) as orders,
                  ROUND(SUM(`order_master`.`order_amt`),2) as revenue,
                  ROUND(SUM(`order_master`.`order_commission`),2) as commission
                  
                  FROM `order_master`
                  
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
                  
                  WHERE `order_master`.`i_date` >= UNIX_TIMESTAMP('2015-04-24')
                  AND `order_master`.`i_date` < UNIX_TIMESTAMP('2015-06-22')
                  AND ((`order_master`.`is_deleted` = 'N') 
                  and ((`order_master`.`status` = 'VERIFIED') 
                  or (`order_master`.`status` = 'REJECTED')))
                  
                  AND `restaurant_master`.`accepts_prepaid_transactions` = 1
                  
                  GROUP BY date, type,account_manager, pref,city 
                  
                  
                  ")


# Fetch query results (n=-1) means all results
prepaid<- dbFetch(rs, n=-1) 
# close connection
dbDisconnect(con)
# Stop timer
proc.time() - ptm



write.csv2(prepaid, file = "export.csv",row.names = FALSE)