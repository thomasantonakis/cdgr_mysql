# Load package
library(RMySQL)

# Set timer
ptm <- proc.time()

# Establish connection
con <- dbConnect(RMySQL::MySQL(), host = '172.20.0.1', port = 3307, dbname = "beta",
                 user = "tantonakis", password = "2secret4usAll!")
# Send query
rs <- dbSendQuery(con,"

SELECT   count(distinct `order_master`.`user_id`) as users


        					FROM `order_master`
						JOIN `order_detail`
						USING (`order_id`)
						JOIN `item_master`
						USING (`item_id`)
						JOIN (`item_detail`)
						USING (`item_id`)
                                                JOIN `user_master`
                                                USING (`user_id`)
						WHERE order_master.i_date > UNIX_TIMESTAMP('2015-03-01')
                                                AND order_master.i_date < UNIX_TIMESTAMP('2015-06-01')
						AND `item_detail`.`language_id` = 1
						AND (`item_detail`.`item_name`  LIKE '%amstel%'
                                                        OR `item_detail`.`item_name`  LIKE '%heineken%'
                                                        OR `item_detail`.`item_name`  LIKE '%μύθος%'
                                                        OR `item_detail`.`item_name`  LIKE '%μυθος%'
                                                        OR `item_detail`.`item_name`  LIKE '%αλφα%'
                                                        OR `item_detail`.`item_name`  LIKE '%άλφα%'
                                                        OR `item_detail`.`item_name`  LIKE '%alfa%'
                                                        OR `item_detail`.`item_name`  LIKE '%kaiser%'
                                                        OR `item_detail`.`item_name`  LIKE '%pils%'
                                                        OR `item_detail`.`item_name`  LIKE '%fischer%'
                                                        OR `item_detail`.`item_name`  LIKE '%fix%'
                                                        OR `item_detail`.`item_name`  LIKE '%φιξ%'
                                                        OR `item_detail`.`item_name`  LIKE '%buckler%'
                                                        OR `item_detail`.`item_name`  LIKE '%εψα%'
                                                        OR `item_detail`.`item_name`  LIKE '%λουξ%'
                                                        OR `item_detail`.`item_name`  LIKE '%sprite%'
                                                        OR `item_detail`.`item_name`  LIKE '%pepsi%'
                                                        OR `item_detail`.`item_name`  LIKE '%coca%'
                                                        OR `item_detail`.`item_name`  LIKE '%fanta%'
                                                        OR `item_detail`.`item_name`  LIKE '%tuborg%'
                                                        OR `item_detail`.`item_name`  LIKE '%hbh%'
                                                        OR `item_detail`.`item_name`  LIKE '%ήβη%'
                                                        OR `item_detail`.`item_name`  LIKE '%ηβη%'
                                                        OR `item_detail`.`item_name`  LIKE '%nestea%'
                                                        OR `item_detail`.`item_name`  LIKE '%icetea%'
                                                        OR `item_detail`.`item_name`  LIKE '%amita%'
                                                        OR `item_detail`.`item_name`  LIKE '%perrier%'
                                                        OR `item_detail`.`item_name`  LIKE '%soda%'
                                                        OR `item_detail`.`item_name`  LIKE '%σοδα%'
                                                        )



                  
                                   ")


# Fetch query results (n=-1) means all results
beverages<- dbFetch(rs, n=-1) 
# close connection
dbDisconnect(con)

# write.csv(x=amstel, file = "amstel.csv", row.names=FALSE)