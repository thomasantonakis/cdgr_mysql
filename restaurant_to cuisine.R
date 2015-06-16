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
        
        count( distinct restaurant_master.restaurant_id) as count,
        MIN(rest_cuisine_out.display_order) AS display_order,
        cuisine_detail.cuisine_id,
        cuisine_detail.cuisine_name
FROM restaurant_master
        
INNER JOIN restaurant_cuisine AS rest_cuisine_out
        USING(restaurant_id)
INNER JOIN cuisine_detail
        USING(cuisine_id)
WHERE 
         rest_cuisine_out.display_order = (
                SELECT MIN(rest_cuisine_in.display_order)
                FROM restaurant_cuisine AS rest_cuisine_in
                WHERE rest_cuisine_in.restaurant_id = rest_cuisine_out.restaurant_id 
        )
        AND cuisine_detail.language_id = 1

group by cuisine_detail.cuisine_name
order by count desc


                  ")
# Fetch query results (n=-1) means all results
rest <- dbFetch(rs, n=-1) 
# close connection
dbDisconnect(con)


# write.csv(rest, file = "export.csv",row.names=FALSE)
# Stop timer
proc.time() - ptm

sum(rest$count)