# Load package
library(RMySQL)
# Set timer
ptm <- proc.time()
# Establish connection
con <- dbConnect(RMySQL::MySQL(), host = '172.20.0.1', port = 3307, dbname = "beta",
                                  user = "tantonakis", password = "2secret4usAll!")
# Send query
rs <- dbSendQuery(con,"


SELECT 
        count(order_master.order_id)as count,
        restaurant_master.restaurant_id,
        MIN(rest_cuisine_out.display_order) AS display_order,
        cuisine_detail.cuisine_id,
        cuisine_detail.cuisine_name
FROM order_master
INNER JOIN restaurant_master
        USING(restaurant_id)
INNER JOIN restaurant_cuisine AS rest_cuisine_out
        USING(restaurant_id)
INNER JOIN cuisine_detail
        USING(cuisine_id)
WHERE order_master.i_date > UNIX_TIMESTAMP('2015-01-01')
        AND order_master.i_date < UNIX_TIMESTAMP('2015-06-01')
        AND order_master.is_deleted = 'N' AND (order_master.status = 'VERIFIED' OR order_master.status = 'REJECTED')
        AND rest_cuisine_out.display_order = (
                SELECT MIN(rest_cuisine_in.display_order)
                FROM restaurant_cuisine AS rest_cuisine_in
                WHERE rest_cuisine_in.restaurant_id = rest_cuisine_out.restaurant_id 
        )
        AND cuisine_detail.language_id = 1
GROUP BY cuisine_detail.cuisine_name
ORDER BY count DESC


                  ")
# Fetch query results (n=-1) means all results
orders <- dbFetch(rs, n=-1) 
# close connection
dbDisconnect(con)



write.csv2(orders, file = "export.csv",row.names=FALSE)
# Stop timer
proc.time() - ptm

# result<-as.data.frame(table(orders$cuisine_name))