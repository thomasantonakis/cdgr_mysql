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
                        count(`order_master`.`order_id`) as orders,
                        ROUND(SUM(`order_master`.`order_amt`),2) as revenue,
                        ROUND(SUM(`order_master`.`order_commission`),2) as commission
                
                FROM `order_master`
                LEFT JOIN `prepayment_transaction`
                ON (`order_master`.`prepayment_transaction_id` = `prepayment_transaction`.`prepayment_transaction_id`)
                LEFT JOIN `user_payment_method` 
                ON (`prepayment_transaction`.`payment_method_id`=`user_payment_method`.`user_payment_method_id`)

                WHERE `order_master`.`i_date` < UNIX_TIMESTAMP('2015-07-01')
                AND ((`order_master`.`is_deleted` = 'N') 
                and ((`order_master`.`status` = 'VERIFIED') 
                        or (`order_master`.`status` = 'REJECTED')))
                AND `order_master`.`prepayment_transaction_id` IS NOT NULL

               GROUP BY date, type
                  
                  
                  
                  ")


# Fetch query results (n=-1) means all results
prepaid<- dbFetch(rs, n=-1) 
# close connection
dbDisconnect(con)
# Stop timer
proc.time() - ptm
