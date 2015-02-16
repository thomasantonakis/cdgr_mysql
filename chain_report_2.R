# Load package
library(RMySQL)
library(plyr)
library(dplyr)
library(tidyr)
library(xlsx)

# Set timer
ptm <- proc.time()

### COmpany report
# Establish connection
con <- dbConnect(RMySQL::MySQL(), host = 'db.clickdelivery.gr', port = 3307, dbname = "beta",
                 user = "tantonakis", password = "2secret4usAll!")
# Send query
rs <- dbSendQuery(con,"

SELECT `restaurant_shortname` AS `RESTAURANT_SHORTNAME`,
                  `restaurant_name` AS `RESTAURANT_NAME`,
                  `restaurantgroup_id` AS `CHAIN_ID`,
                  `city_name` AS `CITY_NAME`,
                  year(FROM_UNIXTIME(`order_master`.`i_date`)) as year,
                  month(FROM_UNIXTIME(`order_master`.`i_date`)) as month,
                  count(`order_master`.`order_id`) AS `ORDERS`,
                  ROUND(SUM(`order_master`.`order_amt`),2) AS `ORDERSUM`,
                  round(sum(`order_master`.`order_commission`), 2) AS `RESTAURANT_COMMISSION`,
                  `prefecture_detail`.`prefecture_name` AS PREFECTURE
                  
                  
                  FROM `order_master`
                  JOIN `restaurant_master`
                  USING (`restaurant_id`)
                  JOIN `restaurant_detail`
                  ON (`restaurant_detail`.`restaurant_id` = `restaurant_master`.`restaurant_id` AND `restaurant_detail`.`language_id` = 1)
                  LEFT JOIN `city_detail`
                  ON (`restaurant_master`.`restaurant_city_id` = `city_detail`.`city_id` AND `city_detail`.`language_id` =1)
                  LEFT JOIN `city_master`
                  ON (`city_master`.`city_id` = `city_detail`.`city_id`)
                  LEFT JOIN `prefecture_detail`
                  ON (`prefecture_detail`.`language_id` = 1 AND `city_master`.`prefecture_id` = `prefecture_detail`.`prefecture_id`)

                  where ((`order_master`.`is_deleted` = 'N') and ((`order_master`.`status` = 'VERIFIED') or (`order_master`.`status` = 'REJECTED')))
                  AND `order_master`.`i_date` >= UNIX_TIMESTAMP('2014-01-01')
                  AND `order_master`.`i_date` < UNIX_TIMESTAMP('2014-02-01')
                  GROUP BY `order_master`.`restaurant_id` 
                  ORDER BY `restaurant_city_id` ASC, `restaurant_shortname` ASC

                  ")

# Fetch query results (n=-1) means all results
company <- dbFetch(rs, n=-1) 
# close connection
dbDisconnect(con)
# Stop timer
proc.time() - ptm


### COmpany report February
# Establish connection
con <- dbConnect(RMySQL::MySQL(), host = 'db.clickdelivery.gr', port = 3307, dbname = "beta",
                 user = "tantonakis", password = "2secret4usAll!")
# Send query
rs <- dbSendQuery(con,"

SELECT `restaurant_shortname` AS `RESTAURANT_SHORTNAME`,
                  `restaurant_name` AS `RESTAURANT_NAME`,
                  `restaurantgroup_id` AS `CHAIN_ID`,
                  `city_name` AS `CITY_NAME`,
                  year(FROM_UNIXTIME(`order_master`.`i_date`)) as year,
                  month(FROM_UNIXTIME(`order_master`.`i_date`)) as month,
                  count(`order_master`.`order_id`) AS `ORDERS`,
                  ROUND(SUM(`order_master`.`order_amt`),2) AS `ORDERSUM`,
                  round(sum(`order_master`.`order_commission`), 2) AS `RESTAURANT_COMMISSION`,
                  `prefecture_detail`.`prefecture_name` AS PREFECTURE
                  
                  
                  FROM `order_master`
                  JOIN `restaurant_master`
                  USING (`restaurant_id`)
                  JOIN `restaurant_detail`
                  ON (`restaurant_detail`.`restaurant_id` = `restaurant_master`.`restaurant_id` AND `restaurant_detail`.`language_id` = 1)
                  LEFT JOIN `city_detail`
                  ON (`restaurant_master`.`restaurant_city_id` = `city_detail`.`city_id` AND `city_detail`.`language_id` =1)
                  LEFT JOIN `city_master`
                  ON (`city_master`.`city_id` = `city_detail`.`city_id`)
                  LEFT JOIN `prefecture_detail`
                  ON (`prefecture_detail`.`language_id` = 1 AND `city_master`.`prefecture_id` = `prefecture_detail`.`prefecture_id`)

                  where ((`order_master`.`is_deleted` = 'N') and ((`order_master`.`status` = 'VERIFIED') or (`order_master`.`status` = 'REJECTED')))
                  AND `order_master`.`i_date` >= UNIX_TIMESTAMP('2014-02-01')
                  AND `order_master`.`i_date` < UNIX_TIMESTAMP('2014-03-01')
                  GROUP BY `order_master`.`restaurant_id` 
                  ORDER BY `restaurant_city_id` ASC, `restaurant_shortname` ASC

                  ")

# Fetch query results (n=-1) means all results
companytemp <- dbFetch(rs, n=-1) 
# close connection
dbDisconnect(con)
# Stop timer
proc.time() - ptm
company<-rbind(company, companytemp)


### COmpany report March
# Establish connection
con <- dbConnect(RMySQL::MySQL(), host = 'db.clickdelivery.gr', port = 3307, dbname = "beta",
                 user = "tantonakis", password = "2secret4usAll!")
# Send query
rs <- dbSendQuery(con,"

SELECT `restaurant_shortname` AS `RESTAURANT_SHORTNAME`,
                  `restaurant_name` AS `RESTAURANT_NAME`,
                  `restaurantgroup_id` AS `CHAIN_ID`,
                  `city_name` AS `CITY_NAME`,
                  year(FROM_UNIXTIME(`order_master`.`i_date`)) as year,
                  month(FROM_UNIXTIME(`order_master`.`i_date`)) as month,
                  count(`order_master`.`order_id`) AS `ORDERS`,
                  ROUND(SUM(`order_master`.`order_amt`),2) AS `ORDERSUM`,
                  round(sum(`order_master`.`order_commission`), 2) AS `RESTAURANT_COMMISSION`,
                  `prefecture_detail`.`prefecture_name` AS PREFECTURE
                  
                  
                  FROM `order_master`
                  JOIN `restaurant_master`
                  USING (`restaurant_id`)
                  JOIN `restaurant_detail`
                  ON (`restaurant_detail`.`restaurant_id` = `restaurant_master`.`restaurant_id` AND `restaurant_detail`.`language_id` = 1)
                  LEFT JOIN `city_detail`
                  ON (`restaurant_master`.`restaurant_city_id` = `city_detail`.`city_id` AND `city_detail`.`language_id` =1)
                  LEFT JOIN `city_master`
                  ON (`city_master`.`city_id` = `city_detail`.`city_id`)
                  LEFT JOIN `prefecture_detail`
                  ON (`prefecture_detail`.`language_id` = 1 AND `city_master`.`prefecture_id` = `prefecture_detail`.`prefecture_id`)

                  where ((`order_master`.`is_deleted` = 'N') and ((`order_master`.`status` = 'VERIFIED') or (`order_master`.`status` = 'REJECTED')))
                  AND `order_master`.`i_date` >= UNIX_TIMESTAMP('2014-03-01')
                  AND `order_master`.`i_date` < UNIX_TIMESTAMP('2014-04-01')
                  GROUP BY `order_master`.`restaurant_id` 
                  ORDER BY `restaurant_city_id` ASC, `restaurant_shortname` ASC

                  ")

# Fetch query results (n=-1) means all results
companytemp <- dbFetch(rs, n=-1) 
# close connection
dbDisconnect(con)
# Stop timer
proc.time() - ptm
company<-rbind(company, companytemp)


### COmpany report April
# Establish connection
con <- dbConnect(RMySQL::MySQL(), host = 'db.clickdelivery.gr', port = 3307, dbname = "beta",
                 user = "tantonakis", password = "2secret4usAll!")
# Send query
rs <- dbSendQuery(con,"

SELECT `restaurant_shortname` AS `RESTAURANT_SHORTNAME`,
                  `restaurant_name` AS `RESTAURANT_NAME`,
                  `restaurantgroup_id` AS `CHAIN_ID`,
                  `city_name` AS `CITY_NAME`,
                  year(FROM_UNIXTIME(`order_master`.`i_date`)) as year,
                  month(FROM_UNIXTIME(`order_master`.`i_date`)) as month,
                  count(`order_master`.`order_id`) AS `ORDERS`,
                  ROUND(SUM(`order_master`.`order_amt`),2) AS `ORDERSUM`,
                  round(sum(`order_master`.`order_commission`), 2) AS `RESTAURANT_COMMISSION`,
                  `prefecture_detail`.`prefecture_name` AS PREFECTURE
                  
                  
                  FROM `order_master`
                  JOIN `restaurant_master`
                  USING (`restaurant_id`)
                  JOIN `restaurant_detail`
                  ON (`restaurant_detail`.`restaurant_id` = `restaurant_master`.`restaurant_id` AND `restaurant_detail`.`language_id` = 1)
                  LEFT JOIN `city_detail`
                  ON (`restaurant_master`.`restaurant_city_id` = `city_detail`.`city_id` AND `city_detail`.`language_id` =1)
                  LEFT JOIN `city_master`
                  ON (`city_master`.`city_id` = `city_detail`.`city_id`)
                  LEFT JOIN `prefecture_detail`
                  ON (`prefecture_detail`.`language_id` = 1 AND `city_master`.`prefecture_id` = `prefecture_detail`.`prefecture_id`)

                  where ((`order_master`.`is_deleted` = 'N') and ((`order_master`.`status` = 'VERIFIED') or (`order_master`.`status` = 'REJECTED')))
                  AND `order_master`.`i_date` >= UNIX_TIMESTAMP('2014-04-01')
                  AND `order_master`.`i_date` < UNIX_TIMESTAMP('2014-05-01')
                  GROUP BY `order_master`.`restaurant_id` 
                  ORDER BY `restaurant_city_id` ASC, `restaurant_shortname` ASC

                  ")

# Fetch query results (n=-1) means all results
companytemp <- dbFetch(rs, n=-1) 
# close connection
dbDisconnect(con)
# Stop timer
proc.time() - ptm
company<-rbind(company, companytemp)

### COmpany report May
# Establish connection
con <- dbConnect(RMySQL::MySQL(), host = 'db.clickdelivery.gr', port = 3307, dbname = "beta",
                 user = "tantonakis", password = "2secret4usAll!")
# Send query
rs <- dbSendQuery(con,"

SELECT `restaurant_shortname` AS `RESTAURANT_SHORTNAME`,
                  `restaurant_name` AS `RESTAURANT_NAME`,
                  `restaurantgroup_id` AS `CHAIN_ID`,
                  `city_name` AS `CITY_NAME`,
                  year(FROM_UNIXTIME(`order_master`.`i_date`)) as year,
                  month(FROM_UNIXTIME(`order_master`.`i_date`)) as month,
                  count(`order_master`.`order_id`) AS `ORDERS`,
                  ROUND(SUM(`order_master`.`order_amt`),2) AS `ORDERSUM`,
                  round(sum(`order_master`.`order_commission`), 2) AS `RESTAURANT_COMMISSION`,
                  `prefecture_detail`.`prefecture_name` AS PREFECTURE
                  
                  
                  FROM `order_master`
                  JOIN `restaurant_master`
                  USING (`restaurant_id`)
                  JOIN `restaurant_detail`
                  ON (`restaurant_detail`.`restaurant_id` = `restaurant_master`.`restaurant_id` AND `restaurant_detail`.`language_id` = 1)
                  LEFT JOIN `city_detail`
                  ON (`restaurant_master`.`restaurant_city_id` = `city_detail`.`city_id` AND `city_detail`.`language_id` =1)
                  LEFT JOIN `city_master`
                  ON (`city_master`.`city_id` = `city_detail`.`city_id`)
                  LEFT JOIN `prefecture_detail`
                  ON (`prefecture_detail`.`language_id` = 1 AND `city_master`.`prefecture_id` = `prefecture_detail`.`prefecture_id`)

                  where ((`order_master`.`is_deleted` = 'N') and ((`order_master`.`status` = 'VERIFIED') or (`order_master`.`status` = 'REJECTED')))
                  AND `order_master`.`i_date` >= UNIX_TIMESTAMP('2014-05-01')
                  AND `order_master`.`i_date` < UNIX_TIMESTAMP('2014-06-01')
                  GROUP BY `order_master`.`restaurant_id` 
                  ORDER BY `restaurant_city_id` ASC, `restaurant_shortname` ASC

                  ")

# Fetch query results (n=-1) means all results
companytemp <- dbFetch(rs, n=-1) 
# close connection
dbDisconnect(con)
# Stop timer
proc.time() - ptm
company<-rbind(company, companytemp)


### COmpany report June
# Establish connection
con <- dbConnect(RMySQL::MySQL(), host = 'db.clickdelivery.gr', port = 3307, dbname = "beta",
                 user = "tantonakis", password = "2secret4usAll!")
# Send query
rs <- dbSendQuery(con,"

SELECT `restaurant_shortname` AS `RESTAURANT_SHORTNAME`,
                  `restaurant_name` AS `RESTAURANT_NAME`,
                  `restaurantgroup_id` AS `CHAIN_ID`,
                  `city_name` AS `CITY_NAME`,
                  year(FROM_UNIXTIME(`order_master`.`i_date`)) as year,
                  month(FROM_UNIXTIME(`order_master`.`i_date`)) as month,
                  count(`order_master`.`order_id`) AS `ORDERS`,
                  ROUND(SUM(`order_master`.`order_amt`),2) AS `ORDERSUM`,
                  round(sum(`order_master`.`order_commission`), 2) AS `RESTAURANT_COMMISSION`,
                  `prefecture_detail`.`prefecture_name` AS PREFECTURE
                  
                  
                  FROM `order_master`
                  JOIN `restaurant_master`
                  USING (`restaurant_id`)
                  JOIN `restaurant_detail`
                  ON (`restaurant_detail`.`restaurant_id` = `restaurant_master`.`restaurant_id` AND `restaurant_detail`.`language_id` = 1)
                  LEFT JOIN `city_detail`
                  ON (`restaurant_master`.`restaurant_city_id` = `city_detail`.`city_id` AND `city_detail`.`language_id` =1)
                  LEFT JOIN `city_master`
                  ON (`city_master`.`city_id` = `city_detail`.`city_id`)
                  LEFT JOIN `prefecture_detail`
                  ON (`prefecture_detail`.`language_id` = 1 AND `city_master`.`prefecture_id` = `prefecture_detail`.`prefecture_id`)

                  where ((`order_master`.`is_deleted` = 'N') and ((`order_master`.`status` = 'VERIFIED') or (`order_master`.`status` = 'REJECTED')))
                  AND `order_master`.`i_date` >= UNIX_TIMESTAMP('2014-06-01')
                  AND `order_master`.`i_date` < UNIX_TIMESTAMP('2014-07-01')
                  GROUP BY `order_master`.`restaurant_id` 
                  ORDER BY `restaurant_city_id` ASC, `restaurant_shortname` ASC

                  ")

# Fetch query results (n=-1) means all results
companytemp <- dbFetch(rs, n=-1) 
# close connection
dbDisconnect(con)
# Stop timer
proc.time() - ptm
company<-rbind(company, companytemp)


### COmpany report July
# Establish connection
con <- dbConnect(RMySQL::MySQL(), host = 'db.clickdelivery.gr', port = 3307, dbname = "beta",
                 user = "tantonakis", password = "2secret4usAll!")
# Send query
rs <- dbSendQuery(con,"

SELECT `restaurant_shortname` AS `RESTAURANT_SHORTNAME`,
                  `restaurant_name` AS `RESTAURANT_NAME`,
                  `restaurantgroup_id` AS `CHAIN_ID`,
                  `city_name` AS `CITY_NAME`,
                  year(FROM_UNIXTIME(`order_master`.`i_date`)) as year,
                  month(FROM_UNIXTIME(`order_master`.`i_date`)) as month,
                  count(`order_master`.`order_id`) AS `ORDERS`,
                  ROUND(SUM(`order_master`.`order_amt`),2) AS `ORDERSUM`,
                  round(sum(`order_master`.`order_commission`), 2) AS `RESTAURANT_COMMISSION`,
                  `prefecture_detail`.`prefecture_name` AS PREFECTURE
                  
                  
                  FROM `order_master`
                  JOIN `restaurant_master`
                  USING (`restaurant_id`)
                  JOIN `restaurant_detail`
                  ON (`restaurant_detail`.`restaurant_id` = `restaurant_master`.`restaurant_id` AND `restaurant_detail`.`language_id` = 1)
                  LEFT JOIN `city_detail`
                  ON (`restaurant_master`.`restaurant_city_id` = `city_detail`.`city_id` AND `city_detail`.`language_id` =1)
                  LEFT JOIN `city_master`
                  ON (`city_master`.`city_id` = `city_detail`.`city_id`)
                  LEFT JOIN `prefecture_detail`
                  ON (`prefecture_detail`.`language_id` = 1 AND `city_master`.`prefecture_id` = `prefecture_detail`.`prefecture_id`)

                  where ((`order_master`.`is_deleted` = 'N') and ((`order_master`.`status` = 'VERIFIED') or (`order_master`.`status` = 'REJECTED')))
                  AND `order_master`.`i_date` >= UNIX_TIMESTAMP('2014-07-01')
                  AND `order_master`.`i_date` < UNIX_TIMESTAMP('2014-08-01')
                  GROUP BY `order_master`.`restaurant_id` 
                  ORDER BY `restaurant_city_id` ASC, `restaurant_shortname` ASC

                  ")

# Fetch query results (n=-1) means all results
companytemp <- dbFetch(rs, n=-1) 
# close connection
dbDisconnect(con)
# Stop timer
proc.time() - ptm
company<-rbind(company, companytemp)


### COmpany report August
# Establish connection
con <- dbConnect(RMySQL::MySQL(), host = 'db.clickdelivery.gr', port = 3307, dbname = "beta",
                 user = "tantonakis", password = "2secret4usAll!")
# Send query
rs <- dbSendQuery(con,"
                  
                  SELECT `restaurant_shortname` AS `RESTAURANT_SHORTNAME`,
                  `restaurant_name` AS `RESTAURANT_NAME`,
                  `restaurantgroup_id` AS `CHAIN_ID`,
                  `city_name` AS `CITY_NAME`,
                  year(FROM_UNIXTIME(`order_master`.`i_date`)) as year,
                  month(FROM_UNIXTIME(`order_master`.`i_date`)) as month,
                  count(`order_master`.`order_id`) AS `ORDERS`,
                  ROUND(SUM(`order_master`.`order_amt`),2) AS `ORDERSUM`,
                  round(sum(`order_master`.`order_commission`), 2) AS `RESTAURANT_COMMISSION`,
                  `prefecture_detail`.`prefecture_name` AS PREFECTURE
                  
                  
                  FROM `order_master`
                  JOIN `restaurant_master`
                  USING (`restaurant_id`)
                  JOIN `restaurant_detail`
                  ON (`restaurant_detail`.`restaurant_id` = `restaurant_master`.`restaurant_id` AND `restaurant_detail`.`language_id` = 1)
                  LEFT JOIN `city_detail`
                  ON (`restaurant_master`.`restaurant_city_id` = `city_detail`.`city_id` AND `city_detail`.`language_id` =1)
                  LEFT JOIN `city_master`
                  ON (`city_master`.`city_id` = `city_detail`.`city_id`)
                  LEFT JOIN `prefecture_detail`
                  ON (`prefecture_detail`.`language_id` = 1 AND `city_master`.`prefecture_id` = `prefecture_detail`.`prefecture_id`)
                  
                  where ((`order_master`.`is_deleted` = 'N') and ((`order_master`.`status` = 'VERIFIED') or (`order_master`.`status` = 'REJECTED')))
                  AND `order_master`.`i_date` >= UNIX_TIMESTAMP('2014-08-01')
                  AND `order_master`.`i_date` < UNIX_TIMESTAMP('2014-09-01')
                  GROUP BY `order_master`.`restaurant_id` 
                  ORDER BY `restaurant_city_id` ASC, `restaurant_shortname` ASC
                  
                  ")

# Fetch query results (n=-1) means all results
companytemp <- dbFetch(rs, n=-1) 
# close connection
dbDisconnect(con)
# Stop timer
proc.time() - ptm
company<-rbind(company, companytemp)


### COmpany report September
# Establish connection
con <- dbConnect(RMySQL::MySQL(), host = 'db.clickdelivery.gr', port = 3307, dbname = "beta",
                 user = "tantonakis", password = "2secret4usAll!")
# Send query
rs <- dbSendQuery(con,"
                  
                  SELECT `restaurant_shortname` AS `RESTAURANT_SHORTNAME`,
                  `restaurant_name` AS `RESTAURANT_NAME`,
                  `restaurantgroup_id` AS `CHAIN_ID`,
                  `city_name` AS `CITY_NAME`,
                  year(FROM_UNIXTIME(`order_master`.`i_date`)) as year,
                  month(FROM_UNIXTIME(`order_master`.`i_date`)) as month,
                  count(`order_master`.`order_id`) AS `ORDERS`,
                  ROUND(SUM(`order_master`.`order_amt`),2) AS `ORDERSUM`,
                  round(sum(`order_master`.`order_commission`), 2) AS `RESTAURANT_COMMISSION`,
                  `prefecture_detail`.`prefecture_name` AS PREFECTURE
                  
                  
                  FROM `order_master`
                  JOIN `restaurant_master`
                  USING (`restaurant_id`)
                  JOIN `restaurant_detail`
                  ON (`restaurant_detail`.`restaurant_id` = `restaurant_master`.`restaurant_id` AND `restaurant_detail`.`language_id` = 1)
                  LEFT JOIN `city_detail`
                  ON (`restaurant_master`.`restaurant_city_id` = `city_detail`.`city_id` AND `city_detail`.`language_id` =1)
                  LEFT JOIN `city_master`
                  ON (`city_master`.`city_id` = `city_detail`.`city_id`)
                  LEFT JOIN `prefecture_detail`
                  ON (`prefecture_detail`.`language_id` = 1 AND `city_master`.`prefecture_id` = `prefecture_detail`.`prefecture_id`)
                  
                  where ((`order_master`.`is_deleted` = 'N') and ((`order_master`.`status` = 'VERIFIED') or (`order_master`.`status` = 'REJECTED')))
                  AND `order_master`.`i_date` >= UNIX_TIMESTAMP('2014-09-01')
                  AND `order_master`.`i_date` < UNIX_TIMESTAMP('2014-10-01')
                  GROUP BY `order_master`.`restaurant_id` 
                  ORDER BY `restaurant_city_id` ASC, `restaurant_shortname` ASC
                  
                  ")

# Fetch query results (n=-1) means all results
companytemp <- dbFetch(rs, n=-1) 
# close connection
dbDisconnect(con)
# Stop timer
proc.time() - ptm
company<-rbind(company, companytemp)


### COmpany report October
# Establish connection
con <- dbConnect(RMySQL::MySQL(), host = 'db.clickdelivery.gr', port = 3307, dbname = "beta",
                 user = "tantonakis", password = "2secret4usAll!")
# Send query
rs <- dbSendQuery(con,"
                  
                  SELECT `restaurant_shortname` AS `RESTAURANT_SHORTNAME`,
                  `restaurant_name` AS `RESTAURANT_NAME`,
                  `restaurantgroup_id` AS `CHAIN_ID`,
                  `city_name` AS `CITY_NAME`,
                  year(FROM_UNIXTIME(`order_master`.`i_date`)) as year,
                  month(FROM_UNIXTIME(`order_master`.`i_date`)) as month,
                  count(`order_master`.`order_id`) AS `ORDERS`,
                  ROUND(SUM(`order_master`.`order_amt`),2) AS `ORDERSUM`,
                  round(sum(`order_master`.`order_commission`), 2) AS `RESTAURANT_COMMISSION`,
                  `prefecture_detail`.`prefecture_name` AS PREFECTURE
                  
                  
                  FROM `order_master`
                  JOIN `restaurant_master`
                  USING (`restaurant_id`)
                  JOIN `restaurant_detail`
                  ON (`restaurant_detail`.`restaurant_id` = `restaurant_master`.`restaurant_id` AND `restaurant_detail`.`language_id` = 1)
                  LEFT JOIN `city_detail`
                  ON (`restaurant_master`.`restaurant_city_id` = `city_detail`.`city_id` AND `city_detail`.`language_id` =1)
                  LEFT JOIN `city_master`
                  ON (`city_master`.`city_id` = `city_detail`.`city_id`)
                  LEFT JOIN `prefecture_detail`
                  ON (`prefecture_detail`.`language_id` = 1 AND `city_master`.`prefecture_id` = `prefecture_detail`.`prefecture_id`)
                  
                  where ((`order_master`.`is_deleted` = 'N') and ((`order_master`.`status` = 'VERIFIED') or (`order_master`.`status` = 'REJECTED')))
                  AND `order_master`.`i_date` >= UNIX_TIMESTAMP('2014-10-01')
                  AND `order_master`.`i_date` < UNIX_TIMESTAMP('2014-11-01')
                  GROUP BY `order_master`.`restaurant_id` 
                  ORDER BY `restaurant_city_id` ASC, `restaurant_shortname` ASC
                  
                  ")

# Fetch query results (n=-1) means all results
companytemp <- dbFetch(rs, n=-1) 
# close connection
dbDisconnect(con)
# Stop timer
proc.time() - ptm
company<-rbind(company, companytemp)


### COmpany report November
# Establish connection
con <- dbConnect(RMySQL::MySQL(), host = 'db.clickdelivery.gr', port = 3307, dbname = "beta",
                 user = "tantonakis", password = "2secret4usAll!")
# Send query
rs <- dbSendQuery(con,"
                  
                  SELECT `restaurant_shortname` AS `RESTAURANT_SHORTNAME`,
                  `restaurant_name` AS `RESTAURANT_NAME`,
                  `restaurantgroup_id` AS `CHAIN_ID`,
                  `city_name` AS `CITY_NAME`,
                  year(FROM_UNIXTIME(`order_master`.`i_date`)) as year,
                  month(FROM_UNIXTIME(`order_master`.`i_date`)) as month,
                  count(`order_master`.`order_id`) AS `ORDERS`,
                  ROUND(SUM(`order_master`.`order_amt`),2) AS `ORDERSUM`,
                  round(sum(`order_master`.`order_commission`), 2) AS `RESTAURANT_COMMISSION`,
                  `prefecture_detail`.`prefecture_name` AS PREFECTURE
                  
                  
                  FROM `order_master`
                  JOIN `restaurant_master`
                  USING (`restaurant_id`)
                  JOIN `restaurant_detail`
                  ON (`restaurant_detail`.`restaurant_id` = `restaurant_master`.`restaurant_id` AND `restaurant_detail`.`language_id` = 1)
                  LEFT JOIN `city_detail`
                  ON (`restaurant_master`.`restaurant_city_id` = `city_detail`.`city_id` AND `city_detail`.`language_id` =1)
                  LEFT JOIN `city_master`
                  ON (`city_master`.`city_id` = `city_detail`.`city_id`)
                  LEFT JOIN `prefecture_detail`
                  ON (`prefecture_detail`.`language_id` = 1 AND `city_master`.`prefecture_id` = `prefecture_detail`.`prefecture_id`)
                  
                  where ((`order_master`.`is_deleted` = 'N') and ((`order_master`.`status` = 'VERIFIED') or (`order_master`.`status` = 'REJECTED')))
                  AND `order_master`.`i_date` >= UNIX_TIMESTAMP('2014-11-01')
                  AND `order_master`.`i_date` < UNIX_TIMESTAMP('2014-12-01')
                  GROUP BY `order_master`.`restaurant_id` 
                  ORDER BY `restaurant_city_id` ASC, `restaurant_shortname` ASC
                  
                  ")

# Fetch query results (n=-1) means all results
companytemp <- dbFetch(rs, n=-1) 
# close connection
dbDisconnect(con)
# Stop timer
proc.time() - ptm
company<-rbind(company, companytemp)


### COmpany report December
# Establish connection
con <- dbConnect(RMySQL::MySQL(), host = 'db.clickdelivery.gr', port = 3307, dbname = "beta",
                 user = "tantonakis", password = "2secret4usAll!")
# Send query
rs <- dbSendQuery(con,"
                  
                  SELECT `restaurant_shortname` AS `RESTAURANT_SHORTNAME`,
                  `restaurant_name` AS `RESTAURANT_NAME`,
                  `restaurantgroup_id` AS `CHAIN_ID`,
                  `city_name` AS `CITY_NAME`,
                  year(FROM_UNIXTIME(`order_master`.`i_date`)) as year,
                  month(FROM_UNIXTIME(`order_master`.`i_date`)) as month,
                  count(`order_master`.`order_id`) AS `ORDERS`,
                  ROUND(SUM(`order_master`.`order_amt`),2) AS `ORDERSUM`,
                  round(sum(`order_master`.`order_commission`), 2) AS `RESTAURANT_COMMISSION`,
                  `prefecture_detail`.`prefecture_name` AS PREFECTURE
                  
                  
                  FROM `order_master`
                  JOIN `restaurant_master`
                  USING (`restaurant_id`)
                  JOIN `restaurant_detail`
                  ON (`restaurant_detail`.`restaurant_id` = `restaurant_master`.`restaurant_id` AND `restaurant_detail`.`language_id` = 1)
                  LEFT JOIN `city_detail`
                  ON (`restaurant_master`.`restaurant_city_id` = `city_detail`.`city_id` AND `city_detail`.`language_id` =1)
                  LEFT JOIN `city_master`
                  ON (`city_master`.`city_id` = `city_detail`.`city_id`)
                  LEFT JOIN `prefecture_detail`
                  ON (`prefecture_detail`.`language_id` = 1 AND `city_master`.`prefecture_id` = `prefecture_detail`.`prefecture_id`)
                  
                  where ((`order_master`.`is_deleted` = 'N') and ((`order_master`.`status` = 'VERIFIED') or (`order_master`.`status` = 'REJECTED')))
                  AND `order_master`.`i_date` >= UNIX_TIMESTAMP('2014-12-01')
                  AND `order_master`.`i_date` < UNIX_TIMESTAMP('2015-01-01')
                  GROUP BY `order_master`.`restaurant_id` 
                  ORDER BY `restaurant_city_id` ASC, `restaurant_shortname` ASC
                  
                  ")

# Fetch query results (n=-1) means all results
companytemp <- dbFetch(rs, n=-1) 
# close connection
dbDisconnect(con)
# Stop timer
proc.time() - ptm
company<-rbind(company, companytemp)


### COmpany report January
# Establish connection
con <- dbConnect(RMySQL::MySQL(), host = 'db.clickdelivery.gr', port = 3307, dbname = "beta",
                 user = "tantonakis", password = "2secret4usAll!")
# Send query
rs <- dbSendQuery(con,"
                  
                  SELECT `restaurant_shortname` AS `RESTAURANT_SHORTNAME`,
                  `restaurant_name` AS `RESTAURANT_NAME`,
                  `restaurantgroup_id` AS `CHAIN_ID`,
                  `city_name` AS `CITY_NAME`,
                  year(FROM_UNIXTIME(`order_master`.`i_date`)) as year,
                  month(FROM_UNIXTIME(`order_master`.`i_date`)) as month,
                  count(`order_master`.`order_id`) AS `ORDERS`,
                  ROUND(SUM(`order_master`.`order_amt`),2) AS `ORDERSUM`,
                  round(sum(`order_master`.`order_commission`), 2) AS `RESTAURANT_COMMISSION`,
                  `prefecture_detail`.`prefecture_name` AS PREFECTURE
                  
                  
                  FROM `order_master`
                  JOIN `restaurant_master`
                  USING (`restaurant_id`)
                  JOIN `restaurant_detail`
                  ON (`restaurant_detail`.`restaurant_id` = `restaurant_master`.`restaurant_id` AND `restaurant_detail`.`language_id` = 1)
                  LEFT JOIN `city_detail`
                  ON (`restaurant_master`.`restaurant_city_id` = `city_detail`.`city_id` AND `city_detail`.`language_id` =1)
                  LEFT JOIN `city_master`
                  ON (`city_master`.`city_id` = `city_detail`.`city_id`)
                  LEFT JOIN `prefecture_detail`
                  ON (`prefecture_detail`.`language_id` = 1 AND `city_master`.`prefecture_id` = `prefecture_detail`.`prefecture_id`)
                  
                  where ((`order_master`.`is_deleted` = 'N') and ((`order_master`.`status` = 'VERIFIED') or (`order_master`.`status` = 'REJECTED')))
                  AND `order_master`.`i_date` >= UNIX_TIMESTAMP('2015-01-01')
                  AND `order_master`.`i_date` < UNIX_TIMESTAMP('2015-02-01')
                  GROUP BY `order_master`.`restaurant_id` 
                  ORDER BY `restaurant_city_id` ASC, `restaurant_shortname` ASC
                  
                  ")

# Fetch query results (n=-1) means all results
companytemp <- dbFetch(rs, n=-1) 
# close connection
dbDisconnect(con)
# Stop timer
proc.time() - ptm

company<-rbind(company, companytemp)
rm(companytemp)


##
# Exclude Chains
chains_analytic<-filter(company, CHAIN_ID != 0)
rest<-filter(company, CHAIN_ID == 0)


### Chain report January
# Establish connection
con <- dbConnect(RMySQL::MySQL(), host = 'db.clickdelivery.gr', port = 3307, dbname = "beta",
                 user = "tantonakis", password = "2secret4usAll!")
# Send query
rs <- dbSendQuery(con,"

SELECT `restaurantgroup_template` AS RESTAURANT_SHORTNAME, 
                                `restaurantgroup_shortname` as RESTAURANT_NAME,
                                `restaurantgroup_id` AS `CHAIN_ID`, 
                                year(FROM_UNIXTIME(`order_master`.`i_date`)) as year,
                                month(FROM_UNIXTIME(`order_master`.`i_date`)) as month,
                                count(`order_master`.`order_id`) AS `ORDERS`,
                		round(sum(`order_master`.`order_amt`), 2) AS `ORDERSUM`, 
                                round(sum(`order_master`.`order_commission`), 2) AS `RESTAURANT_COMMISSION`
                                

				FROM `order_master`
				JOIN `restaurant_master`
				USING (`restaurant_id`)
				JOIN `restaurantgroup`
				USING (`restaurantgroup_id`)
                                
				WHERE ((`order_master`.`is_deleted` = 'N') and ((`order_master`.`status` = 'VERIFIED')
                                        OR (`order_master`.`status` = 'REJECTED')))
				AND `order_master`.`i_date` >= UNIX_TIMESTAMP('2014-01-01')
				AND `order_master`.`i_date` < UNIX_TIMESTAMP('2014-02-01')
				AND `restaurantgroup_id` > 0

				GROUP BY `restaurantgroup_id`

                  ")

# Fetch query results (n=-1) means all results
chain <- dbFetch(rs, n=-1) 
# close connection
dbDisconnect(con)
# Stop timer
proc.time() - ptm


### Chain report February
# Establish connection
con <- dbConnect(RMySQL::MySQL(), host = 'db.clickdelivery.gr', port = 3307, dbname = "beta",
                 user = "tantonakis", password = "2secret4usAll!")
# Send query
rs <- dbSendQuery(con,"
                  
SELECT `restaurantgroup_template` AS RESTAURANT_SHORTNAME, 
                                `restaurantgroup_shortname` as RESTAURANT_NAME,
                                `restaurantgroup_id` AS `CHAIN_ID`, 
                                year(FROM_UNIXTIME(`order_master`.`i_date`)) as year,
                                month(FROM_UNIXTIME(`order_master`.`i_date`)) as month,
                                count(`order_master`.`order_id`) AS `ORDERS`,
                        	round(sum(`order_master`.`order_amt`), 2) AS `ORDERSUM`, 
                                round(sum(`order_master`.`order_commission`), 2) AS `RESTAURANT_COMMISSION`
                                
                 
                  FROM `order_master`
                  JOIN `restaurant_master`
                  USING (`restaurant_id`)
                  JOIN `restaurantgroup`
                  USING (`restaurantgroup_id`)
                  
                  WHERE ((`order_master`.`is_deleted` = 'N') and ((`order_master`.`status` = 'VERIFIED')
                  OR (`order_master`.`status` = 'REJECTED')))
                  AND `order_master`.`i_date` >= UNIX_TIMESTAMP('2014-02-01')
                  AND `order_master`.`i_date` < UNIX_TIMESTAMP('2014-03-01')
                  AND `restaurantgroup_id` > 0
                  
                  GROUP BY `restaurantgroup_id`
                  
                  ")

# Fetch query results (n=-1) means all results
chaintemp <- dbFetch(rs, n=-1) 
# close connection
dbDisconnect(con)
# Stop timer
proc.time() - ptm
chain<-rbind(chain, chaintemp)

### Chain report March
# Establish connection
con <- dbConnect(RMySQL::MySQL(), host = 'db.clickdelivery.gr', port = 3307, dbname = "beta",
                 user = "tantonakis", password = "2secret4usAll!")
# Send query
rs <- dbSendQuery(con,"
                  
SELECT `restaurantgroup_template` AS RESTAURANT_SHORTNAME, 
                                `restaurantgroup_shortname` as RESTAURANT_NAME,
                                `restaurantgroup_id` AS `CHAIN_ID`, 
                                year(FROM_UNIXTIME(`order_master`.`i_date`)) as year,
                                month(FROM_UNIXTIME(`order_master`.`i_date`)) as month,
                                count(`order_master`.`order_id`) AS `ORDERS`,
                        	round(sum(`order_master`.`order_amt`), 2) AS `ORDERSUM`, 
                                round(sum(`order_master`.`order_commission`), 2) AS `RESTAURANT_COMMISSION`
                                
                 
                  FROM `order_master`
                  JOIN `restaurant_master`
                  USING (`restaurant_id`)
                  JOIN `restaurantgroup`
                  USING (`restaurantgroup_id`)
                  
                  WHERE ((`order_master`.`is_deleted` = 'N') and ((`order_master`.`status` = 'VERIFIED')
                  OR (`order_master`.`status` = 'REJECTED')))
                  AND `order_master`.`i_date` >= UNIX_TIMESTAMP('2014-03-01')
                  AND `order_master`.`i_date` < UNIX_TIMESTAMP('2014-04-01')
                  AND `restaurantgroup_id` > 0
                  
                  GROUP BY `restaurantgroup_id`
                  
                  ")

# Fetch query results (n=-1) means all results
chaintemp <- dbFetch(rs, n=-1) 
# close connection
dbDisconnect(con)
# Stop timer
proc.time() - ptm
chain<-rbind(chain, chaintemp)

### Chain report April
# Establish connection
con <- dbConnect(RMySQL::MySQL(), host = 'db.clickdelivery.gr', port = 3307, dbname = "beta",
                 user = "tantonakis", password = "2secret4usAll!")
# Send query
rs <- dbSendQuery(con,"
                  
SELECT `restaurantgroup_template` AS RESTAURANT_SHORTNAME, 
                                `restaurantgroup_shortname` as RESTAURANT_NAME,
                                `restaurantgroup_id` AS `CHAIN_ID`, 
                                year(FROM_UNIXTIME(`order_master`.`i_date`)) as year,
                                month(FROM_UNIXTIME(`order_master`.`i_date`)) as month,
                                count(`order_master`.`order_id`) AS `ORDERS`,
                        	round(sum(`order_master`.`order_amt`), 2) AS `ORDERSUM`, 
                                round(sum(`order_master`.`order_commission`), 2) AS `RESTAURANT_COMMISSION`
                                
                 
                  FROM `order_master`
                  JOIN `restaurant_master`
                  USING (`restaurant_id`)
                  JOIN `restaurantgroup`
                  USING (`restaurantgroup_id`)
                  
                  WHERE ((`order_master`.`is_deleted` = 'N') and ((`order_master`.`status` = 'VERIFIED')
                  OR (`order_master`.`status` = 'REJECTED')))
                  AND `order_master`.`i_date` >= UNIX_TIMESTAMP('2014-04-01')
                  AND `order_master`.`i_date` < UNIX_TIMESTAMP('2014-05-01')
                  AND `restaurantgroup_id` > 0
                  
                  GROUP BY `restaurantgroup_id`
                  
                  ")

# Fetch query results (n=-1) means all results
chaintemp <- dbFetch(rs, n=-1) 
# close connection
dbDisconnect(con)
# Stop timer
proc.time() - ptm
chain<-rbind(chain, chaintemp)

### Chain report May
# Establish connection
con <- dbConnect(RMySQL::MySQL(), host = 'db.clickdelivery.gr', port = 3307, dbname = "beta",
                 user = "tantonakis", password = "2secret4usAll!")
# Send query
rs <- dbSendQuery(con,"

SELECT `restaurantgroup_template` AS RESTAURANT_SHORTNAME, 
                                `restaurantgroup_shortname` as RESTAURANT_NAME,
                                `restaurantgroup_id` AS `CHAIN_ID`, 
                                year(FROM_UNIXTIME(`order_master`.`i_date`)) as year,
                                month(FROM_UNIXTIME(`order_master`.`i_date`)) as month,
                                count(`order_master`.`order_id`) AS `ORDERS`,
                        	round(sum(`order_master`.`order_amt`), 2) AS `ORDERSUM`, 
                                round(sum(`order_master`.`order_commission`), 2) AS `RESTAURANT_COMMISSION`
                                

				FROM `order_master`
				JOIN `restaurant_master`
				USING (`restaurant_id`)
				JOIN `restaurantgroup`
				USING (`restaurantgroup_id`)
                                
				WHERE ((`order_master`.`is_deleted` = 'N') and ((`order_master`.`status` = 'VERIFIED')
                                        OR (`order_master`.`status` = 'REJECTED')))
				AND `order_master`.`i_date` >= UNIX_TIMESTAMP('2014-05-01')
				AND `order_master`.`i_date` < UNIX_TIMESTAMP('2014-06-01')
				AND `restaurantgroup_id` > 0

				GROUP BY `restaurantgroup_id`

                  ")

# Fetch query results (n=-1) means all results
chaintemp <- dbFetch(rs, n=-1) 
# close connection
dbDisconnect(con)
# Stop timer
proc.time() - ptm
chain<-rbind(chain, chaintemp)


### Chain report June
# Establish connection
con <- dbConnect(RMySQL::MySQL(), host = 'db.clickdelivery.gr', port = 3307, dbname = "beta",
                 user = "tantonakis", password = "2secret4usAll!")
# Send query
rs <- dbSendQuery(con,"

SELECT `restaurantgroup_template` AS RESTAURANT_SHORTNAME, 
                                `restaurantgroup_shortname` as RESTAURANT_NAME,
                                `restaurantgroup_id` AS `CHAIN_ID`, 
                                year(FROM_UNIXTIME(`order_master`.`i_date`)) as year,
                                month(FROM_UNIXTIME(`order_master`.`i_date`)) as month,
                                count(`order_master`.`order_id`) AS `ORDERS`,
                        	round(sum(`order_master`.`order_amt`), 2) AS `ORDERSUM`, 
                                round(sum(`order_master`.`order_commission`), 2) AS `RESTAURANT_COMMISSION`
                                

				FROM `order_master`
				JOIN `restaurant_master`
				USING (`restaurant_id`)
				JOIN `restaurantgroup`
				USING (`restaurantgroup_id`)
                                
				WHERE ((`order_master`.`is_deleted` = 'N') and ((`order_master`.`status` = 'VERIFIED')
                                        OR (`order_master`.`status` = 'REJECTED')))
				AND `order_master`.`i_date` >= UNIX_TIMESTAMP('2014-06-01')
				AND `order_master`.`i_date` < UNIX_TIMESTAMP('2014-07-01')
				AND `restaurantgroup_id` > 0

				GROUP BY `restaurantgroup_id`

                  ")

# Fetch query results (n=-1) means all results
chaintemp <- dbFetch(rs, n=-1) 
# close connection
dbDisconnect(con)
# Stop timer
proc.time() - ptm
chain<-rbind(chain, chaintemp)

### Chain report July
# Establish connection
con <- dbConnect(RMySQL::MySQL(), host = 'db.clickdelivery.gr', port = 3307, dbname = "beta",
                 user = "tantonakis", password = "2secret4usAll!")
# Send query
rs <- dbSendQuery(con,"

SELECT `restaurantgroup_template` AS RESTAURANT_SHORTNAME, 
                                `restaurantgroup_shortname` as RESTAURANT_NAME,
                                `restaurantgroup_id` AS `CHAIN_ID`, 
                                year(FROM_UNIXTIME(`order_master`.`i_date`)) as year,
                                month(FROM_UNIXTIME(`order_master`.`i_date`)) as month,
                                count(`order_master`.`order_id`) AS `ORDERS`,
                        	round(sum(`order_master`.`order_amt`), 2) AS `ORDERSUM`, 
                                round(sum(`order_master`.`order_commission`), 2) AS `RESTAURANT_COMMISSION`
                                
                              

				FROM `order_master`
				JOIN `restaurant_master`
				USING (`restaurant_id`)
				JOIN `restaurantgroup`
				USING (`restaurantgroup_id`)
                                
				WHERE ((`order_master`.`is_deleted` = 'N') and ((`order_master`.`status` = 'VERIFIED')
                                        OR (`order_master`.`status` = 'REJECTED')))
				AND `order_master`.`i_date` >= UNIX_TIMESTAMP('2014-07-01')
				AND `order_master`.`i_date` < UNIX_TIMESTAMP('2014-08-01')
				AND `restaurantgroup_id` > 0

				GROUP BY `restaurantgroup_id`

                  ")

# Fetch query results (n=-1) means all results
chaintemp <- dbFetch(rs, n=-1) 
# close connection
dbDisconnect(con)
# Stop timer
proc.time() - ptm
chain<-rbind(chain, chaintemp)

### Chain report August
# Establish connection
con <- dbConnect(RMySQL::MySQL(), host = 'db.clickdelivery.gr', port = 3307, dbname = "beta",
                 user = "tantonakis", password = "2secret4usAll!")
# Send query
rs <- dbSendQuery(con,"
                  
SELECT `restaurantgroup_template` AS RESTAURANT_SHORTNAME, 
                                `restaurantgroup_shortname` as RESTAURANT_NAME,
                                `restaurantgroup_id` AS `CHAIN_ID`, 
                                year(FROM_UNIXTIME(`order_master`.`i_date`)) as year,
                                month(FROM_UNIXTIME(`order_master`.`i_date`)) as month,
                                count(`order_master`.`order_id`) AS `ORDERS`,
                        	round(sum(`order_master`.`order_amt`), 2) AS `ORDERSUM`, 
                                round(sum(`order_master`.`order_commission`), 2) AS `RESTAURANT_COMMISSION`
                                
                
                  FROM `order_master`
                  JOIN `restaurant_master`
                  USING (`restaurant_id`)
                  JOIN `restaurantgroup`
                  USING (`restaurantgroup_id`)
                  
                  WHERE ((`order_master`.`is_deleted` = 'N') and ((`order_master`.`status` = 'VERIFIED')
                  OR (`order_master`.`status` = 'REJECTED')))
                  AND `order_master`.`i_date` >= UNIX_TIMESTAMP('2014-08-01')
                  AND `order_master`.`i_date` < UNIX_TIMESTAMP('2014-09-01')
                  AND `restaurantgroup_id` > 0
                  
                  GROUP BY `restaurantgroup_id`
                  
                  ")

# Fetch query results (n=-1) means all results
chaintemp <- dbFetch(rs, n=-1) 
# close connection
dbDisconnect(con)
# Stop timer
proc.time() - ptm
chain<-rbind(chain, chaintemp)

### Chain report September
# Establish connection
con <- dbConnect(RMySQL::MySQL(), host = 'db.clickdelivery.gr', port = 3307, dbname = "beta",
                 user = "tantonakis", password = "2secret4usAll!")
# Send query
rs <- dbSendQuery(con,"

SELECT `restaurantgroup_template` AS RESTAURANT_SHORTNAME, 
                                `restaurantgroup_shortname` as RESTAURANT_NAME,
                                `restaurantgroup_id` AS `CHAIN_ID`, 
                                year(FROM_UNIXTIME(`order_master`.`i_date`)) as year,
                                month(FROM_UNIXTIME(`order_master`.`i_date`)) as month,
                                count(`order_master`.`order_id`) AS `ORDERS`,
                        	round(sum(`order_master`.`order_amt`), 2) AS `ORDERSUM`, 
                                round(sum(`order_master`.`order_commission`), 2) AS `RESTAURANT_COMMISSION`
                                

				FROM `order_master`
				JOIN `restaurant_master`
				USING (`restaurant_id`)
				JOIN `restaurantgroup`
				USING (`restaurantgroup_id`)
                                
				WHERE ((`order_master`.`is_deleted` = 'N') and ((`order_master`.`status` = 'VERIFIED')
                                        OR (`order_master`.`status` = 'REJECTED')))
				AND `order_master`.`i_date` >= UNIX_TIMESTAMP('2014-09-01')
				AND `order_master`.`i_date` < UNIX_TIMESTAMP('2014-10-01')
				AND `restaurantgroup_id` > 0

				GROUP BY `restaurantgroup_id`

                  ")

# Fetch query results (n=-1) means all results
chaintemp <- dbFetch(rs, n=-1) 
# close connection
dbDisconnect(con)
# Stop timer
proc.time() - ptm
chain<-rbind(chain, chaintemp)

### Chain report October
# Establish connection
con <- dbConnect(RMySQL::MySQL(), host = 'db.clickdelivery.gr', port = 3307, dbname = "beta",
                 user = "tantonakis", password = "2secret4usAll!")
# Send query
rs <- dbSendQuery(con,"


SELECT `restaurantgroup_template` AS RESTAURANT_SHORTNAME, 
                                `restaurantgroup_shortname` as RESTAURANT_NAME,
                                `restaurantgroup_id` AS `CHAIN_ID`, 
                                year(FROM_UNIXTIME(`order_master`.`i_date`)) as year,
                                month(FROM_UNIXTIME(`order_master`.`i_date`)) as month,
                                count(`order_master`.`order_id`) AS `ORDERS`,
                        	round(sum(`order_master`.`order_amt`), 2) AS `ORDERSUM`, 
                                round(sum(`order_master`.`order_commission`), 2) AS `RESTAURANT_COMMISSION`
                                              
                  
                  FROM `order_master`
                  JOIN `restaurant_master`
                  USING (`restaurant_id`)
                  JOIN `restaurantgroup`
                  USING (`restaurantgroup_id`)
                  
                  WHERE ((`order_master`.`is_deleted` = 'N') and ((`order_master`.`status` = 'VERIFIED')
                  OR (`order_master`.`status` = 'REJECTED')))
                  AND `order_master`.`i_date` >= UNIX_TIMESTAMP('2014-10-01')
                  AND `order_master`.`i_date` < UNIX_TIMESTAMP('2014-11-01')
                  AND `restaurantgroup_id` > 0
                  
                  GROUP BY `restaurantgroup_id`
                  
                  ")

# Fetch query results (n=-1) means all results
chaintemp <- dbFetch(rs, n=-1) 
# close connection
dbDisconnect(con)
# Stop timer
proc.time() - ptm
chain<-rbind(chain, chaintemp)

### Chain report November
# Establish connection
con <- dbConnect(RMySQL::MySQL(), host = 'db.clickdelivery.gr', port = 3307, dbname = "beta",
                 user = "tantonakis", password = "2secret4usAll!")
# Send query
rs <- dbSendQuery(con,"
                  
SELECT `restaurantgroup_template` AS RESTAURANT_SHORTNAME, 
                                `restaurantgroup_shortname` as RESTAURANT_NAME,
                                `restaurantgroup_id` AS `CHAIN_ID`, 
                                year(FROM_UNIXTIME(`order_master`.`i_date`)) as year,
                                month(FROM_UNIXTIME(`order_master`.`i_date`)) as month,
                                count(`order_master`.`order_id`) AS `ORDERS`,
                        	round(sum(`order_master`.`order_amt`), 2) AS `ORDERSUM`, 
                                round(sum(`order_master`.`order_commission`), 2) AS `RESTAURANT_COMMISSION`
                                
             
                  
                  FROM `order_master`
                  JOIN `restaurant_master`
                  USING (`restaurant_id`)
                  JOIN `restaurantgroup`
                  USING (`restaurantgroup_id`)
                  
                  WHERE ((`order_master`.`is_deleted` = 'N') and ((`order_master`.`status` = 'VERIFIED')
                  OR (`order_master`.`status` = 'REJECTED')))
                  AND `order_master`.`i_date` >= UNIX_TIMESTAMP('2014-11-01')
                  AND `order_master`.`i_date` < UNIX_TIMESTAMP('2014-12-01')
                  AND `restaurantgroup_id` > 0
                  
                  GROUP BY `restaurantgroup_id`
                  
                  ")

# Fetch query results (n=-1) means all results
chaintemp <- dbFetch(rs, n=-1) 
# close connection
dbDisconnect(con)
# Stop timer
proc.time() - ptm
chain<-rbind(chain, chaintemp)

### Chain report December
# Establish connection
con <- dbConnect(RMySQL::MySQL(), host = 'db.clickdelivery.gr', port = 3307, dbname = "beta",
                 user = "tantonakis", password = "2secret4usAll!")
# Send query
rs <- dbSendQuery(con,"
                  
SELECT `restaurantgroup_template` AS RESTAURANT_SHORTNAME, 
                                `restaurantgroup_shortname` as RESTAURANT_NAME,
                                `restaurantgroup_id` AS `CHAIN_ID`, 
                                year(FROM_UNIXTIME(`order_master`.`i_date`)) as year,
                                month(FROM_UNIXTIME(`order_master`.`i_date`)) as month,
                                count(`order_master`.`order_id`) AS `ORDERS`,
                        	round(sum(`order_master`.`order_amt`), 2) AS `ORDERSUM`, 
                                round(sum(`order_master`.`order_commission`), 2) AS `RESTAURANT_COMMISSION`
                                
               
                  FROM `order_master`
                  JOIN `restaurant_master`
                  USING (`restaurant_id`)
                  JOIN `restaurantgroup`
                  USING (`restaurantgroup_id`)
                  
                  WHERE ((`order_master`.`is_deleted` = 'N') and ((`order_master`.`status` = 'VERIFIED')
                  OR (`order_master`.`status` = 'REJECTED')))
                  AND `order_master`.`i_date` >= UNIX_TIMESTAMP('2014-12-01')
                  AND `order_master`.`i_date` < UNIX_TIMESTAMP('2015-01-01')
                  AND `restaurantgroup_id` > 0
                  
                  GROUP BY `restaurantgroup_id`
                  
                  ")

# Fetch query results (n=-1) means all results
chaintemp <- dbFetch(rs, n=-1) 
# close connection
dbDisconnect(con)
# Stop timer
proc.time() - ptm
chain<-rbind(chain, chaintemp)

### Chain report January
# Establish connection
con <- dbConnect(RMySQL::MySQL(), host = 'db.clickdelivery.gr', port = 3307, dbname = "beta",
                 user = "tantonakis", password = "2secret4usAll!")
# Send query
rs <- dbSendQuery(con,"
                  
                  SELECT `restaurantgroup_template` AS RESTAURANT_SHORTNAME, 
                  `restaurantgroup_shortname` as RESTAURANT_NAME,
                  `restaurantgroup_id` AS `CHAIN_ID`, 
                  year(FROM_UNIXTIME(`order_master`.`i_date`)) as year,
                  month(FROM_UNIXTIME(`order_master`.`i_date`)) as month,
                  count(`order_master`.`order_id`) AS `ORDERS`,
                  round(sum(`order_master`.`order_amt`), 2) AS `ORDERSUM`, 
                  round(sum(`order_master`.`order_commission`), 2) AS `RESTAURANT_COMMISSION`
                  
                  
                  FROM `order_master`
                  JOIN `restaurant_master`
                  USING (`restaurant_id`)
                  JOIN `restaurantgroup`
                  USING (`restaurantgroup_id`)
                  
                  WHERE ((`order_master`.`is_deleted` = 'N') and ((`order_master`.`status` = 'VERIFIED')
                  OR (`order_master`.`status` = 'REJECTED')))
                  AND `order_master`.`i_date` >= UNIX_TIMESTAMP('2015-01-01')
                  AND `order_master`.`i_date` < UNIX_TIMESTAMP('2015-02-01')
                  AND `restaurantgroup_id` > 0
                  
                  GROUP BY `restaurantgroup_id`
                  
                  ")

# Fetch query results (n=-1) means all results
chaintemp <- dbFetch(rs, n=-1) 
# close connection
dbDisconnect(con)
# Stop timer
proc.time() - ptm
chain<-rbind(chain, chaintemp)
rm(chaintemp)

# Analysis & Preparation for Exports

company$monthofyear<-company$month
company$monthofyear[company$month<10]<-paste("0",company$month[company$month<10], sep="")
company$monthofyear<-paste(company$year,company$monthofyear, sep="")

# comp_ord<-ddply(company,c("monthofyear"), summarize, orders=sum(ORDERS))
# orders<-data.frame(matrix(0, nrow=1, ncol=nrow(comp_ord)))
# names(orders)<-names(comp_ord %>% spread(monthofyear, orders))
# orders[1,]<-diag(as.matrix(comp_ord %>% spread(monthofyear, orders)))
# orders$type<-"company"

orders<-ddply(company,c("PREFECTURE","monthofyear"), summarize, orders=sum(ORDERS)) %>% spread(monthofyear, orders)
orders[is.na(orders)]<-0
orders$type<-"company"
# orders$metric<-"orders"
# orders<-orders[,c(1, ncol(orders), 2:(ncol(orders)-1))]

# comp_sal<-ddply(company,c("monthofyear"), summarize, sales=sum(ORDERSUM))
# revenue<-data.frame(matrix(0, nrow=1, ncol=nrow(comp_sal)))
# names(revenue)<-names(comp_sal %>% spread(monthofyear, sales))
# revenue[1,]<-diag(as.matrix(comp_sal %>% spread(monthofyear, sales)))
# revenue$type<-"company"

revenue<-ddply(company,c("PREFECTURE","monthofyear"), summarize, revenue=sum(ORDERSUM)) %>% spread(monthofyear, revenue)
revenue[is.na(revenue)]<-0
revenue$type<-"company"
# revenue$metric<-"revenue"
# revenue<-revenue[,c(1, ncol(revenue), 2:(ncol(revenue)-1))]

# comp_com<-ddply(company,c("monthofyear"), summarize, comm=sum(RESTAURANT_COMMISSION))
# commission<-data.frame(matrix(0, nrow=1, ncol=nrow(comp_com)))
# names(commission)<-names(comp_com %>% spread(monthofyear, comm))
# commission[1,]<-diag(as.matrix(comp_com %>% spread(monthofyear, comm)))
# commission$type<-"company"

commission<-ddply(company,c("PREFECTURE","monthofyear"), summarize, commission=sum(RESTAURANT_COMMISSION)) %>% spread(monthofyear, commission)
commission[is.na(commission)]<-0
commission$type<-"company"
# commission$metric<-"commission"
# commission<-commission[,c(1, ncol(commission), 2:(ncol(commission)-1))]



# comp_num<-as.data.frame(table(company$monthofyear))
# names(comp_num)<-c("monthofyear", "restaurants")
# # Add for Pizza Fan 60 restaurants
# comp_num$restaurants<-comp_num$restaurants+60
# comp_num$type<-"company"
# restaurants<-comp_num %>% spread(monthofyear, restaurants)
# restaurants<-restaurants[,c(2:ncol(restaurants), 1)]

restaurants<-as.data.frame(table(company$PREFECTURE, company$monthofyear, useNA= "ifany"))%>% spread(Var2,  Freq)
names(restaurants)[1]<-"PREFECTURE"
# Add for Pizza Fan 60 restaurants
restaurants[is.na(restaurants$PREFECTURE),2:ncol(restaurants)]<-restaurants[is.na(restaurants$PREFECTURE),2:ncol(restaurants)]+60
restaurants$type<-"company"
# restaurants$metric<-"restaurants"
# restaurants<-restaurants[,c(1, ncol(restaurants), 2:(ncol(restaurants)-1))]


chains_analytic$monthofyear<-chains_analytic$month
chains_analytic$monthofyear[chains_analytic$month<10]<-paste("0",chains_analytic$month[chains_analytic$month<10], sep="")
chains_analytic$monthofyear<-paste(chains_analytic$year,chains_analytic$monthofyear, sep="")

chain$monthofyear<-chain$month
chain$monthofyear[chain$month<10]<-paste("0",chain$month[chain$month<10], sep="")
chain$monthofyear<-paste(chain$year,chain$monthofyear, sep="")

chain_ord<-select(chain,CHAIN_ID,monthofyear, ORDERS)   %>% spread(monthofyear, ORDERS)
chain_ord[is.na(chain_ord)]<-0
chain_ord$type<-"chain"
names(chain_ord)[1]<-"PREFECTURE"
chain_ord<-chain_ord[,c(2:(ncol(chain_ord)-1), 1, ncol(chain_ord))]

chain_sal<-select(chain,CHAIN_ID,monthofyear, ORDERSUM)   %>% spread(monthofyear, ORDERSUM)
chain_sal[is.na(chain_sal)]<-0
chain_sal$type<-"chain"
names(chain_sal)[1]<-"PREFECTURE"
chain_sal<-chain_sal[,c(2:(ncol(chain_sal)-1), 1, ncol(chain_sal))]

chain_com<-select(chain,CHAIN_ID,monthofyear, RESTAURANT_COMMISSION)   %>% spread(monthofyear, RESTAURANT_COMMISSION)
chain_com[is.na(chain_com)]<-0
chain_com$type<-"chain"
names(chain_com)[1]<-"PREFECTURE"
chain_com<-chain_com[,c(2:(ncol(chain_com)-1), 1, ncol(chain_com))]

chain_res<-as.data.frame(table(chains_analytic$CHAIN_ID,chains_analytic$monthofyear ))
names(chain_res)<-c("PREFECTURE", "monthofyear", "num")
chain_res<-chain_res %>% spread(monthofyear, num)
chain_res$type<-"chain"
chain_res<-chain_res[,c(2:(ncol(chain_res)-1), 1, ncol(chain_res))]

orders<-rbind(orders, chain_ord)
orders$metric<-"orders"
revenue<-rbind(revenue, chain_sal)
revenue$metric<-"revenue"
commission<-rbind(commission, chain_com)
commission$metric<-"commission"
restaurants<-rbind(restaurants, chain_res)
restaurants$metric<-"restaurants"

# orders<-orders[,c((ncol(orders)-1):ncol(orders), 1:(ncol(orders)-2))]
# revenue<-revenue[,c((ncol(revenue)-1):ncol(revenue), 1:(ncol(revenue)-2))]
# commission<-commission[,c((ncol(commission)-1):ncol(commission), 1:(ncol(commission)-2))]
# restaurants<-restaurants[,c(1, ncol(restaurants), 2:(ncol(restaurants)-1))]
# Add on Pizza Fan 60 restaurants
restaurants[restaurants$PREFECTURE==41 & !is.na(restaurants$PREFECTURE),2:(ncol(restaurants)-2)]<-
        restaurants[restaurants$PREFECTURE==41 & !is.na(restaurants$PREFECTURE),2:(ncol(restaurants)-2)]+60

report<-rbind(orders, revenue, commission, restaurants)
report<-report[,c(1, (ncol(report)-1):ncol(report), 2:(ncol(report)-2))]

#Export
write.xlsx(x=report, file="chain2.xlsx", sheetName = "Data", row.names=FALSE)

# Stop timer
proc.time() - ptm