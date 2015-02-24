# Load package
library(RMySQL)
# Set timer
ptm <- proc.time()
# Establish connection
con <- dbConnect(RMySQL::MySQL(), host = '172.20.0.1', port = 3307, dbname = "beta",
                                  user = "tantonakis", password = "2secret4usAll!")
# Send query
rs <- dbSendQuery(con,"

SELECT COUNT(*) AS VERIFIED_USERS,
        `user_master`.`referal_source` AS SOURCE,
        `city_detail`.`city_name` AS CITY, 
        `prefecture_detail`.`prefecture_name` AS PREFECTURE

                FROM `user_master`
		LEFT JOIN `user_address`
                ON (`user_address`.`is_default` = 'Y' AND `user_address`.`user_id` = `user_master`.`user_id`)
                LEFT JOIN `city_master`
                ON (`user_address`.`city_id` = `city_master`.`city_id`)
                LEFT JOIN `city_detail`
                ON (`city_detail`.`language_id` = '2' AND `city_master`.`city_id` = `city_detail`.`city_id`)
                LEFT JOIN `prefecture_detail`
                ON (`prefecture_detail`.`language_id` = '2' AND `city_master`.`prefecture_id` = `prefecture_detail`.`prefecture_id`)

                  WHERE `user_master`.`verification_date` >= UNIX_TIMESTAMP('2015-01-01')
                  AND `user_master`.`verification_date` < UNIX_TIMESTAMP('2015-02-01')
                  AND `user_master`.`status` = 'VERIFIED'
                  AND `user_master`.`is_deleted` = 'N'
                  GROUP BY `user_master`.`referal_source`, `city_master`.`city_id`

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

