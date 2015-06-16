# Load package
library(RMySQL)

# Set timer
ptm <- proc.time()

# Establish connection
con <- dbConnect(RMySQL::MySQL(), host = '172.20.0.1', port = 3307, dbname = "beta",
                 user = "tantonakis", password = "2secret4usAll!")
# Send query
rs <- dbSendQuery(con,"

SELECT  YEAR(FROM_UNIXTIME(`order_master`.`i_date`)) AS YEAR, 
        MONTH(FROM_UNIXTIME(`order_master`.`i_date`)) AS MONTH,
        COUNT(DISTINCT `order_id`) AS Order_count

        					FROM `order_master`
						JOIN `order_detail`
						USING (`order_id`)
						JOIN `item_master`
						USING (`item_id`)
						JOIN (`item_detail`)
						USING (`item_id`)
						WHERE `order_master`.`i_date` > UNIX_TIMESTAMP('2014-01-01')
						AND `order_master`.`i_date` < UNIX_TIMESTAMP('2015-04-01')
						AND `item_detail`.`language_id` = 2
						AND (`item_detail`.`item_name`  LIKE '%λουξ%'
                                                OR `item_detail`.`item_name`  LIKE '%εψα%')

        GROUP BY YEAR, MONTH

                  ")

# Fetch query results (n=-1) means all results
orders <- dbFetch(rs, n=-1) 
# close connection
dbDisconnect(con)
# Stop timer
proc.time() - ptm