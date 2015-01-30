# Load package
library(RMySQL)
# Set timer
ptm <- proc.time()
# Establish connection
con <- dbConnect(RMySQL::MySQL(), host = 'db.clickdelivery.gr', port = 3307, dbname = "beta",
                                  user = "tantonakis", password = "2secret4usAll!")
# Send query
rs <- dbSendQuery(con,"

SELECT  FROM_UNIXTIME(`i_date`), count(`order_id`), sum(`order_amt`), sum(`order_commission`)
FROM `order_master`
WHERE  `i_date` >= UNIX_TIMESTAMP('2015-01-24')
AND `i_date` < UNIX_TIMESTAMP('2015-01-26')
and `status` IN (
'VERIFIED',  'REJECTED'
)
GROUP BY  FROM_UNIXTIME(`i_date`)
                  ")
# Fetch query results (n=-1) means all results
d1 <- dbFetch(rs, n=-1) 
# close connection
dbDisconnect(con)
# Stop timer
proc.time() - ptm

library(lubridate)
d1$day <- wday(d1$'FROM_UNIXTIME(`i_date`)', label = TRUE, abbr = TRUE)
names(d1)<- c("second", "orders", "sales", "commission", "day")
library(plyr)
ddply(d1,.(day),summarize,orders = sum(orders), sales = sum(sales), commission = sum(commission))

