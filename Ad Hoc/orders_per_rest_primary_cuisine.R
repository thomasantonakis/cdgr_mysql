# Load package
library(RMySQL)
# Set timer
ptm <- proc.time()
# Establish connection
con <- dbConnect(RMySQL::MySQL(), host = 'db.clickdelivery.gr', port = 3307, dbname = "beta",
                 user = "tantonakis", password = "2secret4usAll!")
# Send query
rs <- dbSendQuery(con, "
                  
                  
SELECT  `cuisine_detail`.`cuisine_name` AS cuisine,
        COUNT(`order_master`.`order_id`) AS orders
        

FROM `order_master`
        JOIN `restaurant_master`
        ON (`order_master`.`restaurant_id` = `restaurant_master`.`restaurant_id`)
        JOIN `restaurant_detail`
        ON (`restaurant_detail`.`restaurant_id` = `restaurant_master`.`restaurant_id` AND `restaurant_detail`.`language_id` = 1)
        JOIN `restaurant_cuisine`
        ON ( `restaurant_master`.`restaurant_id` = `restaurant_cuisine`.`restaurant_id`)
        JOIN `cuisine_detail`
        ON (`restaurant_cuisine`.`cuisine_id` = `cuisine_detail`.`cuisine_id` AND `cuisine_detail`.`language_id` =1 )

WHERE  `order_master`.`i_date` >= UNIX_TIMESTAMP('2015-01-01')
AND `order_master`.`i_date` < UNIX_TIMESTAMP('2015-02-16')
AND  ((`order_master`.`is_deleted` = 'N') and ((`order_master`.`status` = 'VERIFIED') or (`order_master`.`status` = 'REJECTED')))
AND `restaurant_cuisine`.`display_order` = 1
AND `restaurant_detail`.`language_id` = 1

GROUP BY cuisine 

ORDER by orders DESC
                  
                  ")
# Fetch query results (n=-1) means all results
cuisine <- dbFetch(rs, n=-1) 
# close connection
dbDisconnect(con)
# Stop timer
proc.time() - ptm

cuisine$pct<-round(cuisine$orders/sum(cuisine$orders)*100, 1)
cuisine$cumulative<-cumsum(cuisine$pct)
sum(cuisine$orders)
View(cuisine)
par(las=2) # make label text perpendicular to axis

par(mar=c(6,4,2.1,1))
barplot(cuisine$pct, names.arg=cuisine$cuisine, col=3, main="Cuisine Share in orders of 2015",
         ylab="% Share in orders of 2015", ylim=c(0,max(cuisine$pct)+5))

