# Load package
library(RMySQL)

# Set timer
ptm <- proc.time()

### COmpany report
# Establish connection
con <- dbConnect(RMySQL::MySQL(), host = '172.20.0.1', port = 3307, dbname = "beta",
                 user = "tantonakis", password = "2secret4usAll!")
# Send query
rs <- dbSendQuery(con,"
                  
                  SELECT `user_payment_method` .`method_type` as type,
                        count( `order_master`.`order_id`) AS `ORDERS`
                

                  
                  FROM `order_master`
                left JOIN `prepayment_transaction`
                        ON (`prepayment_transaction`.`prepayment_transaction_id`
                        = `order_master`.`prepayment_transaction_id`)
                left join `user_payment_method`
                        ON (`prepayment_transaction`.`payment_method_id`
                        = `user_payment_method`.`user_payment_method_id`)
                  
                  where ((`order_master`.`is_deleted` = 'N') and ((`order_master`.`status` = 'VERIFIED') or (`order_master`.`status` = 'REJECTED')))
                  AND `order_master`.`i_date` >= UNIX_TIMESTAMP('2015-04-24')
                  AND `order_master`.`prepayment_transaction_id` is not null or `order_master`.`pay_with_ticket_restaurant`<>0
                 
                  GROUP BY `type`

                  ")

# Fetch query results (n=-1) means all results
orders <- dbFetch(rs, n=-1) 
# close connection
dbDisconnect(con)
# Stop timer
proc.time() - ptm
orders[is.na(orders)]<-'ticket'
View(orders)

## COmpany report
# Establish connection
con <- dbConnect(RMySQL::MySQL(), host = '172.20.0.1', port = 3307, dbname = "beta",
                 user = "tantonakis", password = "2secret4usAll!")
# Send query
rs <- dbSendQuery(con,"
                  
                  SELECT 
                        count( `order_master`.`order_id`) AS `ORDERS`
                

                  
                  FROM `order_master`
                
                  
                  where ((`order_master`.`is_deleted` = 'N') and ((`order_master`.`status` = 'VERIFIED') or (`order_master`.`status` = 'REJECTED')))
                  AND `order_master`.`i_date` >= UNIX_TIMESTAMP('2015-04-24')               
                  

                  ")

# Fetch query results (n=-1) means all results
totals <- dbFetch(rs, n=-1) 
# close connection
dbDisconnect(con)
sum(orders$ORDERS)
round(100*sum(orders$ORDERS)/totals,2)
# Stop timer
proc.time() - ptm