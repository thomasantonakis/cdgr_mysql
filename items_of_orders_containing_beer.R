# Load package
library(RMySQL)

# Set timer
ptm <- proc.time()

# Establish connection
con <- dbConnect(RMySQL::MySQL(), host = '172.20.0.1', port = 3307, dbname = "beta",
                 user = "tantonakis", password = "2secret4usAll!")
# Send query
rs <- dbSendQuery(con,"
                  
                  
SELECT `order_detail`.`order_id` ,
                  `order_detail`.`item_id` ,
                  `order_detail`.`item_qty` ,
                  `order_detail`.`total_amt` ,
                  `item_detail`.`item_name`,
                  `category_detail`.`category_name` as Cat
                  
                  FROM `order_detail`
                  
                  JOIN (
                  SELECT
                  YEAR(FROM_UNIXTIME(`order_master`.`i_date`)) AS YEAR, 
                  MONTH(FROM_UNIXTIME(`order_master`.`i_date`)) AS MONTH,
                  `order_master`.`order_id`,
                  `item_detail`.`item_name`,
                  `category_detail`.`category_name`
                  FROM `order_master`
                  JOIN `order_detail`
                  ON (`order_master`.`order_id` = `order_detail`.`order_id`)
                  JOIN (`item_master`)
                  ON (`order_detail`.`item_id` = `item_master`.`item_id`)
                  JOIN (`item_detail`)
                  ON (`item_detail`.`item_id` = `order_detail`.`item_id` AND `item_detail`.`language_id` = 1)
                  JOIN (`category_detail`)
                  ON (`category_detail`.`category_id` = `item_master`.`category_id` AND `category_detail`.`language_id`=1 )
                  WHERE order_master.i_date > UNIX_TIMESTAMP('2014-01-01')
                  AND order_master.i_date < UNIX_TIMESTAMP('2015-04-01')
                  AND order_master.is_deleted = 'N' AND (order_master.status = 'VERIFIED' OR order_master.status = 'REJECTED')
                  AND (`item_detail`.`item_name`  LIKE '%amstel%'
                  OR `item_detail`.`item_name`  LIKE '%heineken%'
                  OR `item_detail`.`item_name`  LIKE '%μύθος%'
                  OR `item_detail`.`item_name`  LIKE '%μυθος%'
                  OR `item_detail`.`item_name`  LIKE '%αλφα%'
                  OR `item_detail`.`item_name`  LIKE '%άλφα%'
                  OR `item_detail`.`item_name`  LIKE '%alfa%'
                  OR `item_detail`.`item_name`  LIKE '%kaiser%'
                  OR `item_detail`.`item_name`  LIKE '%pils%'
                  OR `item_detail`.`item_name`  LIKE '%fischer%'
                  OR `item_detail`.`item_name`  LIKE '%fix%'
                  OR `item_detail`.`item_name`  LIKE '%φιξ%'
                  OR `item_detail`.`item_name`  LIKE '%mythos%'
                  OR `item_detail`.`item_name`  LIKE '%buckler%'
                  )
                  
                  GROUP BY `order_master`.`order_id`
                  ) ORDERS
                  
                  ON (`order_detail`.`order_id` = ORDERS.`order_id`)
                  JOIN (`item_master`)
                  ON (`order_detail`.`item_id` = `item_master`.`item_id`)
                  JOIN (`item_detail`)
                  ON (`item_detail`.`item_id` = `order_detail`.`item_id` AND `item_detail`.`language_id` = 1)
                  JOIN (`category_detail`)
                  ON (`category_detail`.`category_id` = `item_master`.`category_id` AND `category_detail`.`language_id`=1 )
                  
                  
               
                  ORDER BY `order_detail`.`order_id` 
                  
                  
                  ")

# Fetch query results (n=-1) means all results
data <- dbFetch(rs, n=-1) 
# close connection
dbDisconnect(con)
# Stop timer
proc.time() - ptm
