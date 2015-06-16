# Load package
library(RMySQL)
library(plyr)
library(xlsx)

# Set timer
ptm <- proc.time()

# Establish connection
con <- dbConnect(RMySQL::MySQL(), host = '172.20.0.1', port = 3307, dbname = "beta",
                 user = "tantonakis", password = "2secret4usAll!")
# Send query
rs <- dbSendQuery(con,"

SELECT  `restaurant_shortname` AS restaurant, MONTHNAME( FROM_UNIXTIME(  `order_master`.`i_date` ) ) AS 
MONTH , YEAR( FROM_UNIXTIME(  `order_master`.`i_date` ) ) AS YEAR, COUNT( * ) AS orders, SUM(  `order_amt` ) AS totals, SUM(  `order_commission` ) AS commissions
FROM  `order_master` 
JOIN  `restaurant_master` 
USING (  `restaurant_id` ) 
WHERE  `restaurant_shortname` LIKE  'goodys%'
AND (
        `order_master`.`status` =  'VERIFIED'
        OR  `order_master`.`status` =  'REJECTED'
)
AND FROM_UNIXTIME(  `order_master`.`i_date` ) >=  '2015-06-01'
AND FROM_UNIXTIME(  `order_master`.`i_date` ) <  '2015-07-01'
GROUP BY  `restaurant_shortname`  
ORDER BY  `restaurant_shortname` 

                  
                  ")
# Fetch query results (n=-1) means all results
data <- dbFetch(rs, n=-1) 
# close connection
dbDisconnect(con)
# Stop timer
proc.time() - ptm

write.xlsx(x=data, file='goodys.xlsx')

