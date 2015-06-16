# Load package
library(RMySQL)

# Set timer
ptm <- proc.time()
# Establish connection
con <- dbConnect(RMySQL::MySQL(), host = '172.20.0.1', port = 3307, dbname = "beta",
                 user = "tantonakis", password = "2secret4usAll!")
# Send query
rs <- dbSendQuery(con, "
                  

SELECT YEAR(FROM_UNIXTIME(`user_master`.`i_date`)) AS YEAR,
                        MONTH(FROM_UNIXTIME(`user_master`.`i_date`)) AS MONTH,
                        `city_detail`.`city_name` AS CITY_NAME,
                        `prefecture_detail`.`prefecture_name` AS PREFECTURE, 
                        count(DISTINCT `user_master`.`user_id`) AS USERS

FROM `user_master`

LEFT JOIN `user_address`
ON (`user_master`.`user_id` = `user_address`.`user_id`)
LEFT JOIN `city_master`
ON (`user_address`.`city_id` = `city_master`.`city_id`)
LEFT JOIN `city_detail`
ON (`city_detail`.`language_id` = 1 AND `city_master`.`city_id` = `city_detail`.`city_id`)
LEFT JOIN `prefecture_detail`
ON (`prefecture_detail`.`language_id` = 1 AND `city_master`.`prefecture_id` = `prefecture_detail`.`prefecture_id`)

WHERE `user_master`.`is_deleted` = 'N' 
       AND `user_master`.`i_date` >= UNIX_TIMESTAMP('2009-01-01')
       AND `user_master`.`i_date` < UNIX_TIMESTAMP('2015-06-01')


GROUP BY YEAR, MONTH, CITY_NAME

ORDER BY YEAR DESC, MONTH DESC, PREFECTURE ASC,  CITY_NAME ASC

                  ")
# Fetch query results (n=-1) means all results
reg_per_area <- dbFetch(rs, n=-1) 
# close connection
dbDisconnect(con)
# Stop timer
proc.time() - ptm


write.csv(reg_per_area, file = "export.csv",row.names=FALSE)