# Load package
library(RMySQL)

# Set timer
ptm <- proc.time()

### COmpany report

# Establish connection
con <- dbConnect(RMySQL::MySQL(), host = '172.20.0.1', port = 3307, dbname = "beta",
                 user = "tantonakis", password = "2secret4usAll!")
# Send query
rs <- dbSendQuery(con,"

SELECT COUNT(distinct `user_id`) AS distinct_users, 
                `order_master`.`order_referal` AS SOURCE, 
                year(from_unixtime(`order_master`.`i_date`)) as year,
                month(from_unixtime(`order_master`.`i_date`)) as month
                

FROM `order_master`


WHERE `order_master`.`i_date` >= UNIX_TIMESTAMP('2014-01-01')
AND `order_master`.`i_date` < UNIX_TIMESTAMP('2016-01-01')
AND `order_master`.`status` IN ('VERIFIED', 'REJECTED')
AND   `order_referal` LIKE ( 'android%')
AND `order_master`.`is_deleted` = 'N'

GROUP BY  year, month


                  ")


# Fetch query results (n=-1) means all results
android<- dbFetch(rs, n=-1) 
# close connection
dbDisconnect(con)

# Establish connection
con <- dbConnect(RMySQL::MySQL(), host = '172.20.0.1', port = 3307, dbname = "beta",
                 user = "tantonakis", password = "2secret4usAll!")
# Send query
rs <- dbSendQuery(con,"

SELECT COUNT(distinct `user_id`) AS distinct_users, 
                `order_master`.`order_referal` AS SOURCE, 
                year(from_unixtime(`order_master`.`i_date`)) as year,
                month(from_unixtime(`order_master`.`i_date`)) as month
                

FROM `order_master`


WHERE `order_master`.`i_date` >= UNIX_TIMESTAMP('2014-01-01')
AND `order_master`.`i_date` < UNIX_TIMESTAMP('2016-01-01')
AND `order_master`.`status` IN ('VERIFIED', 'REJECTED')
AND   `order_referal` LIKE ('ios%')
AND `order_master`.`is_deleted` = 'N'

GROUP BY  year, month


                  ")


# Fetch query results (n=-1) means all results
ios<- dbFetch(rs, n=-1) 
# close connection
dbDisconnect(con)


# Establish connection
con <- dbConnect(RMySQL::MySQL(), host = '172.20.0.1', port = 3307, dbname = "beta",
                 user = "tantonakis", password = "2secret4usAll!")
# Send query
rs <- dbSendQuery(con,"

SELECT COUNT(distinct `user_id`) AS distinct_users, 
                `order_master`.`order_referal` AS SOURCE, 
                year(from_unixtime(`order_master`.`i_date`)) as year,
                month(from_unixtime(`order_master`.`i_date`)) as month
                

FROM `order_master`


WHERE `order_master`.`i_date` >= UNIX_TIMESTAMP('2014-01-01')
AND `order_master`.`i_date` < UNIX_TIMESTAMP('2016-01-01')
AND `order_master`.`status` IN ('VERIFIED', 'REJECTED')
AND (  `order_referal` LIKE ('ios%')
        OR `order_referal` LIKE ('android%'))
AND `order_master`.`is_deleted` = 'N'

GROUP BY  year, month


                  ")


# Fetch query results (n=-1) means all results
mobile<- dbFetch(rs, n=-1) 
# close connection
dbDisconnect(con)

# Establish connection
con <- dbConnect(RMySQL::MySQL(), host = '172.20.0.1', port = 3307, dbname = "beta",
                 user = "tantonakis", password = "2secret4usAll!")
# Send query
rs <- dbSendQuery(con,"

SELECT COUNT(distinct `user_id`) AS distinct_users, 
                year(from_unixtime(`order_master`.`i_date`)) as year,
                month(from_unixtime(`order_master`.`i_date`)) as month
                

FROM `order_master`


WHERE `order_master`.`i_date` >= UNIX_TIMESTAMP('2014-01-01')
AND `order_master`.`i_date` < UNIX_TIMESTAMP('2016-01-01')
AND `order_master`.`status` IN ('VERIFIED', 'REJECTED')
AND `order_referal` NOT LIKE ('ios%')
AND `order_referal` NOT LIKE ('android%')
AND `order_master`.`is_deleted` = 'N'

GROUP BY  year, month


                  ")


# Fetch query results (n=-1) means all results
web<- dbFetch(rs, n=-1) 
# close connection
dbDisconnect(con)
# Stop timer
proc.time() - ptm

# Establish connection
con <- dbConnect(RMySQL::MySQL(), host = '172.20.0.1', port = 3307, dbname = "beta",
                 user = "tantonakis", password = "2secret4usAll!")
# Send query
rs <- dbSendQuery(con,"

SELECT COUNT(distinct `user_id`) AS distinct_users, 
                year(from_unixtime(`order_master`.`i_date`)) as year,
                month(from_unixtime(`order_master`.`i_date`)) as month
                

FROM `order_master`


WHERE `order_master`.`i_date` >= UNIX_TIMESTAMP('2014-01-01')
AND `order_master`.`i_date` < UNIX_TIMESTAMP('2016-01-01')
AND `order_master`.`status` IN ('VERIFIED', 'REJECTED')
AND `order_master`.`is_deleted` = 'N'

GROUP BY  year, month


                  ")


# Fetch query results (n=-1) means all results
platform<- dbFetch(rs, n=-1) 
# close connection
dbDisconnect(con)
# Stop timer
proc.time() - ptm