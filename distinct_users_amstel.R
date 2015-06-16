# Load package
library(RMySQL)

# Set timer
ptm <- proc.time()

# Establish connection
con <- dbConnect(RMySQL::MySQL(), host = '172.20.0.1', port = 3307, dbname = "beta",
                 user = "tantonakis", password = "2secret4usAll!")
# Send query
rs <- dbSendQuery(con,"

SELECT   `order_master`.`user_id`,
`user_master`.`email_id`,
`user_master`.`mobile`

        					FROM `order_master`
						JOIN `order_detail`
						USING (`order_id`)
						JOIN `item_master`
						USING (`item_id`)
						JOIN (`item_detail`)
						USING (`item_id`)
                                                JOIN `user_master`
                                                USING (`user_id`)
						WHERE order_master.i_date > UNIX_TIMESTAMP('2014-05-01')
                                                AND order_master.i_date < UNIX_TIMESTAMP('2015-05-01')
						AND `item_detail`.`language_id` = 1
						AND `item_detail`.`item_name`  LIKE '%amstel%'
GROUP BY `order_master`.`user_id`
                  
                                   ")


# Fetch query results (n=-1) means all results
amstel<- dbFetch(rs, n=-1) 
# close connection
dbDisconnect(con)

write.csv(x=amstel, file = "amstel.csv", row.names=FALSE)