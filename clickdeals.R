# Load package
library(RMySQL)

# Set timer
ptm <- proc.time()

# Establish connection
con <- dbConnect(RMySQL::MySQL(), host = '172.20.0.1', port = 3307, dbname = "beta",
                 user = "tantonakis", password = "2secret4usAll!")
# Send query
rs <- dbSendQuery(con,"
                  
                  
SELECT `order_master`.`order_id` ,  
                  year(FROM_UNIXTIME(`order_master`.`i_date`))AS year,
                  month(FROM_UNIXTIME(`order_master`.`i_date`))AS month
                  
                  
                  FROM  `order_master`
                  JOIN `order_detail`
                  USING (`order_id`)
                  JOIN (`item_master`)
                  ON (`order_detail`.`item_id` = `item_master`.`item_id`)
                  JOIN (`item_detail`)
                  ON (`item_detail`.`item_id` = `order_detail`.`item_id` AND `item_detail`.`language_id` = 1)
                  JOIN (`category_detail`)
                  ON (`category_detail`.`category_id` = `item_master`.`category_id` AND `category_detail`.`language_id`=1 )
                  
                  WHERE `order_master`.`i_date` >= UNIX_TIMESTAMP('2015-01-01')
                  AND `order_master`.`i_date` < UNIX_TIMESTAMP('2015-07-01')
                  AND ((`order_master`.`is_deleted` = 'N') and ((`order_master`.`status` = 'VERIFIED') or (`order_master`.`status` = 'REJECTED')))
                  AND category_detail.category_name  LIKE 'CLICK DEAL%'

                  
                  
                  ")

# Fetch query results (n=-1) means all results
data <- dbFetch(rs, n=-1) 
# close connection
dbDisconnect(con)
# Stop timer
proc.time() - ptm
