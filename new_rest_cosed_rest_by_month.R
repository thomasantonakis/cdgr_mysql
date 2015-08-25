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
                  year(`restaurant_master`.`start_date`)AS year,
                  month(`restaurant_master`.`start_date`)AS month,
                  count(`restaurant_master`.`restaurant_id`) as new
                  
                  FROM `restaurant_master`
                  
                  WHERE `restaurant_master`.`start_date` >= '2013-01-01'
                        AND `country_id`=0
                  
                  GROUP BY year, month
                  
                  ")
# Fetch query results (n=-1) means all results
start <- dbFetch(rs, n=-1) 
# close connection
dbDisconnect(con)
# Stop timer
proc.time() - ptm


# Load package
# library(RMySQL)
# Set timer
# ptm <- proc.time()
# Establish connection
con <- dbConnect(RMySQL::MySQL(), host = '172.20.0.1', port = 3307, dbname = "beta",
                 user = "tantonakis", password = "2secret4usAll!")
# Send query
rs <- dbSendQuery(con,"
                  
                  SELECT 
                  year(`restaurant_master`.`signoff_date`)AS year,
                  month(`restaurant_master`.`signoff_date`)AS month,
                  count(`restaurant_master`.`restaurant_id`) as churn
                  
                  FROM `restaurant_master`
                  
                  WHERE `restaurant_master`.`signoff_date` >= '2013-01-01'
                        AND `country_id`=0
                  
                  GROUP BY year, month
                  
                  ")
# Fetch query results (n=-1) means all results
close <- dbFetch(rs, n=-1) 
# close connection
dbDisconnect(con)
# Stop timer
proc.time() - ptm

export<-merge(start, close)

write.csv2(x=export, file="export.csv", row.names = FALSE)