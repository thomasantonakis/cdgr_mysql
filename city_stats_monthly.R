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
                  
                SELECT  `date` , 
                        `city_stats`.`prefecture_id` as pref_id,
                        `verified_orders` as orders, 
                        `android_orders` as android, 
                        `ios_orders` as ios, 
                        `registrations` as reg, 
                        `verifications` as ver, 
                        `total_sales` as rev, 
                        `total_commissions` as com, 
                        `thisyear_orders` as cy,
                        `lastyear_orders` as ly,

                `prefecture_detail`.`prefecture_name` as pref_name


                FROM  `city_stats` 
                LEFT JOIN `prefecture_detail`
                ON (`prefecture_detail`.`language_id` = 1 AND `city_stats`.`prefecture_id` = `prefecture_detail`.`prefecture_id`)

                  
                  
                WHERE `date` >=  '2015-02-01' AND `date` <=  '2015-02-28' 
                  
                ORDER BY date ASC
                  
                  
                  ")
# Fetch query results (n=-1) means all results
data <- dbFetch(rs, n=-1) 
# close connection
dbDisconnect(con)
# Stop timer
proc.time() - ptm


write.xlsx(x=data, file='citystats_0215.xlsx')

