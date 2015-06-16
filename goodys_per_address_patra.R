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

SELECT
        `user_address`.`address_id` as id,
        `user_address`.`road_name` as road,
        `user_address`.`road_no` as number,
        `user_address`.`letter` as letter,
        `user_address`.`municipality` as municipality,
        count(`order_master`.`order_id`) as orders,
        round(sum(`order_master`.`order_amt`) ,2)as revenue,
        `order_master`.`restaurant_id` as rest,
        `restaurant_master`.`restaurant_shortname` as restaurant,
        month(FROM_UNIXTIME(`order_master`.`i_date`)) as month,
        year(FROM_UNIXTIME(`order_master`.`i_date`)) as year

        FROM `order_master` 
        LEFT JOIN `user_address` 
        ON ( `order_master`.`deliveryaddress_id` = `user_address`.`address_id`)
        LEFT JOIN `restaurant_master`
        ON (`order_master`.`restaurant_id` = `restaurant_master`.`restaurant_id`)

        WHERE( (`order_master`.`restaurant_id` = 1468 ) OR(`order_master`.`restaurant_id` = 1469) )
        AND ((`order_master`.`is_deleted` = 'N') and ((`order_master`.`status` = 'VERIFIED') or (`order_master`.`status` = 'REJECTED')))
        AND `order_master`.`i_date` >= UNIX_TIMESTAMP('2015-01-01')
	AND `order_master`.`i_date` < UNIX_TIMESTAMP('2015-05-01')

        GROUP BY id, year, month

        ORDER BY revenue DESC


 ")

# Fetch query results (n=-1) means all results
patra_goodys <- dbFetch(rs, n=-1) 
# close connection
dbDisconnect(con)
# Stop timer
proc.time() - ptm

# write.xlsx(x= coupons, file= "working.xlsx", sheetName="Worksheet", row.names=FALSE)