# Load package
library(RMySQL)
library(xlsx)
library(plyr)
library(dplyr)
# Set timer
ptm <- proc.time()
# Establish connection
con <- dbConnect(RMySQL::MySQL(), host = '172.20.0.1', port = 3307, dbname = "beta",
                 user = "tantonakis", password = "2secret4usAll!")
# Send query
rs <- dbSendQuery(con,"

SELECT  `order_master`.`restaurant_id` ,
        `restaurant_detail`.`restaurant_name`,
        `order_master`.`user_id` ,
        `user_master`.`mobile`,
        `user_master`.`email_id`,
        FROM_UNIXTIME(`order_master`.`i_date`) as order_date,
        `order_master`.`status`, 
        `prefecture_detail`.`prefecture_name` AS PREFECTURE,
        round(sum(`order_master`.`order_amt`), 2) as revenue,
        round(sum(`order_master`.`order_commission`), 2) as commission ,
        COUNT(DISTINCT `order_master`.`restaurant_id`) AS distinct_rest,
        COUNT(DISTINCT `order_master`.`order_id`) AS distinct_ord,
        COUNT(DISTINCT `order_master`.`deliveryaddress_id`) AS distinct_add
        

FROM  `order_master` 
                  JOIN `restaurant_master`
                  USING (`restaurant_id`)
                  JOIN `user_master`
                  ON `order_master`.`user_id` = `user_master`.`user_id`
                  JOIN `restaurant_detail`
                  ON (`restaurant_detail`.`restaurant_id` = `restaurant_master`.`restaurant_id` AND `restaurant_detail`.`language_id` = 1)
                  LEFT JOIN `city_detail`
                  ON (`restaurant_master`.`restaurant_city_id` = `city_detail`.`city_id` AND `city_detail`.`language_id` =1)
                  LEFT JOIN `city_master`
                  ON (`city_master`.`city_id` = `city_detail`.`city_id`)
                  LEFT JOIN `prefecture_detail`
                  ON (`prefecture_detail`.`language_id` = 1 AND `city_master`.`prefecture_id` = `prefecture_detail`.`prefecture_id`)



WHERE  `order_master`.`i_date` >= UNIX_TIMESTAMP('2014-11-1')
and ((`order_master`.`is_deleted` = 'N') and ((`order_master`.`status` = 'VERIFIED') or (`order_master`.`status` = 'REJECTED')))
and  `restaurant_detail`.`language_id` = 1

GROUP BY `order_master`.`user_id`  
order by revenue DESC
                  

                  ")
# Fetch query results (n=-1) means all results
peruser <- dbFetch(rs, n=-1) 
# close connection
dbDisconnect(con)
# Stop timer
proc.time() - ptm


### Order alternative
# Establish connection
con <- dbConnect(RMySQL::MySQL(), host = '172.20.0.1', port = 3307, dbname = "beta",
                 user = "tantonakis", password = "2secret4usAll!")
# Send query
rs <- dbSendQuery(con,"

SELECT `order_master`.`restaurant_id` ,
        `restaurant_detail`.`restaurant_name`,
        `order_master`.`user_id` ,
        FROM_UNIXTIME(`order_master`.`i_date`) as order_date,
        `order_master`.`status`, 
        `user_master`.`mobile`,
        `user_master`.`email_id`,
        `prefecture_detail`.`prefecture_name` AS PREFECTURE,
        round(`order_master`.`order_amt`, 2) as revenue,
        round(`order_master`.`order_commission`, 2) as commission ,
        `order_master`.`deliveryaddress_id` as address
        
        

FROM  `order_master` 
                  JOIN `restaurant_master`
                  USING (`restaurant_id`)
                  JOIN `user_master`
                  ON `order_master`.`user_id` = `user_master`.`user_id`
                  JOIN `restaurant_detail`
                  ON (`restaurant_detail`.`restaurant_id` = `restaurant_master`.`restaurant_id` AND `restaurant_detail`.`language_id` = 1)
                  LEFT JOIN `city_detail`
                  ON (`restaurant_master`.`restaurant_city_id` = `city_detail`.`city_id` AND `city_detail`.`language_id` =1)
                  LEFT JOIN `city_master`
                  ON (`city_master`.`city_id` = `city_detail`.`city_id`)
                  LEFT JOIN `prefecture_detail`
                  ON (`prefecture_detail`.`language_id` = 1 AND `city_master`.`prefecture_id` = `prefecture_detail`.`prefecture_id`)

WHERE  `order_master`.`i_date` >= UNIX_TIMESTAMP('2014-11-1')
and ((`order_master`.`is_deleted` = 'N') and ((`order_master`.`status` = 'VERIFIED') or (`order_master`.`status` = 'REJECTED')))
and  `restaurant_detail`.`language_id` = 1


order by revenue DESC
                  

                  ")
# Fetch query results (n=-1) means all results
orders <- dbFetch(rs, n=-1) 
# close connection
dbDisconnect(con)
# Stop timer
proc.time() - ptm

####################
#### ANALYSIS
####################

peruser$restaurant_id<-NULL
peruser$restaurant_name<-NULL
peruser$status<-NULL
peruser$order_date<-NULL

serres_summary<-filter(peruser,PREFECTURE == 'Serres' )
patra_summary<-filter(peruser,PREFECTURE == 'Patra' )
alexandroupoli_summary<-filter(peruser,PREFECTURE == 'Alexandroupoli' )
xanthi_summary<-filter(peruser,PREFECTURE == 'Xanthi' )



serres_orders<-filter(orders,PREFECTURE == 'Serres' )
patra_orders<-filter(orders,PREFECTURE == 'Patra' )
alexandroupoli_orders<-filter(orders,PREFECTURE == 'Alexandroupoli' )
xanthi_orders<-filter(orders,PREFECTURE == 'Xanthi' )

alexandroupoli_summary<-alexandroupoli_summary[order(alexandroupoli_summary$distinct_ord, decreasing = TRUE),] 
patra_summary<-patra_summary[order(patra_summary$distinct_ord, decreasing = TRUE),] 
serres_summary<-serres_summary[order(serres_summary$distinct_ord, decreasing = TRUE),] 
xanthi_summary<-xanthi_summary[order(xanthi_summary$distinct_ord, decreasing = TRUE),] 

# Merge dataframes for Top 10
howmany<-10

alexandroupoli<-as.data.frame(head(alexandroupoli_summary$user_id, howmany))
names(alexandroupoli)<-"user_id"
sum(alexandroupoli_summary$distinct_ord[1:howmany])
alexandroupoli<-merge(alexandroupoli, alexandroupoli_orders)

patra<-as.data.frame(head(patra_summary$user_id, howmany))
names(patra)<-"user_id"
sum(patra_summary$distinct_ord[1:howmany])
patra<-merge(patra, patra_orders)

serres<-as.data.frame(head(serres_summary$user_id, howmany))
names(serres)<-"user_id"
sum(serres_summary$distinct_ord[1:howmany])
serres<-merge(serres, serres_orders)

xanthi<-as.data.frame(head(xanthi_summary$user_id, howmany))
names(xanthi)<-"user_id"
sum(xanthi_summary$distinct_ord[howmany])
xanthi<-merge(xanthi, xanthi_orders)

test<-as.data.frame(head(alexandroupoli_summary, howmany))
report<-test
test<-as.data.frame(head(patra_summary, howmany))
report<-rbind(report, test)
test<-as.data.frame(head(serres_summary, howmany))
report<-rbind(report, test)
test<-as.data.frame(head(xanthi_summary, howmany))
report<-rbind(report, test)
rm(test)



#Export
write.xlsx(x=report, file="top10.xlsx", sheetName = "Data", row.names=FALSE)

# Stop timer
proc.time() - ptm