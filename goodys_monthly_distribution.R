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

SELECT  MONTHNAME( FROM_UNIXTIME(  `order_master`.`i_date` ) ) AS 
MONTH , YEAR( FROM_UNIXTIME(  `order_master`.`i_date` ) ) AS YEAR, `order_amt` 
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


                  
                  ")
# Fetch query results (n=-1) means all results
data <- dbFetch(rs, n=-1) 
# close connection
dbDisconnect(con)
# Stop timer
proc.time() - ptm

# write.xlsx(x=data, file='goodys.xlsx')
summary(data$order_amt)
round(sum(data$order_amt<12)/dim(data)[1]*100,2)

hist(data$order_amt[data$order_amt<=30], breaks = seq(7, 31, by = 1), freq=F, main="June Goodys orders distribution", 
     xlab="Basket")
abline(v=8, col="red")