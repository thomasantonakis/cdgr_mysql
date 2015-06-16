#########################################
############### Monthly #################
#########################################

#########################################
# Verified Users From mySQL
#########################################

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

                  WHERE `user_master`.`verification_date` >= UNIX_TIMESTAMP('2015-05-01')
                  AND `user_master`.`verification_date` < UNIX_TIMESTAMP('2015-05-15')
                  AND `user_master`.`status` = 'VERIFIED'
                  AND `user_master`.`is_deleted` = 'N'
                  GROUP BY `user_master`.`referal_source`, `city_master`.`city_id`

                  ")
# Fetch query results (n=-1) means all results
verified_src <- dbFetch(rs, n=-1) 

# close connection
dbDisconnect(con)
# Stop timer
proc.time() - ptm


#########################################
# Registered Users From mySQL
#########################################

# Establish connection
con <- dbConnect(RMySQL::MySQL(), host = '172.20.0.1', port = 3307, dbname = "beta",
                 user = "tantonakis", password = "2secret4usAll!")
# Send query
rs <- dbSendQuery(con,"

SELECT COUNT(*) AS REGISTERED_USERS, `user_master`.`status`, `user_master`.`referal_source` AS SOURCE
        					FROM `user_master`
						WHERE `user_master`.`i_date` >= UNIX_TIMESTAMP('2015-05-01')
						AND `user_master`.`i_date` < UNIX_TIMESTAMP('2015-05-15')
						AND `user_master`.`is_deleted` = 'N'
						GROUP BY `user_master`.`status`, `user_master`.`referal_source`

                  ")
# Fetch query results (n=-1) means all results
registered_src <- dbFetch(rs, n=-1) 

# close connection
dbDisconnect(con)
# Stop timer
proc.time() - ptm


#########################################
# Orders From mySQL
#########################################

# Establish connection
con <- dbConnect(RMySQL::MySQL(), host = '172.20.0.1', port = 3307, dbname = "beta",
                 user = "tantonakis", password = "2secret4usAll!")
# Send query
rs <- dbSendQuery(con,"

SELECT COUNT(*) AS VERIFIED_ORDERS, 
`order_master`.`order_referal` AS SOURCE, 
`city_detail`.`city_name` AS CITY, 
`prefecture_detail`.`prefecture_name` AS PREFECTURE, 
SUM(`order_master`.`order_amt`) AS ORDER_VALUE, 
SUM(`order_master`.`order_commission`) AS COMMISSION
        					FROM `order_master`
						JOIN `user_address`
						ON (`order_master`.`deliveryaddress_id` = `user_address`.`address_id`)
						JOIN `city_master`
						ON (`user_address`.`city_id` = `city_master`.`city_id`)
						JOIN `city_detail`
						ON (`city_detail`.`language_id` = '2' AND `user_address`.`city_id` = `city_detail`.`city_id`)
						JOIN `prefecture_detail`
						ON (`prefecture_detail`.`language_id` = '2' AND `city_master`.`prefecture_id` = `prefecture_detail`.`prefecture_id`)
						WHERE `order_master`.`i_date` >= UNIX_TIMESTAMP('2015-05-01')
						AND `order_master`.`i_date` < UNIX_TIMESTAMP('2015-05-15')
						AND `order_master`.`status` IN ('VERIFIED', 'REJECTED')
						AND `order_master`.`is_deleted` = 'N'
						GROUP BY `order_master`.`status`, `order_master`.`order_referal`, `city_detail`.`city_id`

                  ")
# Fetch query results (n=-1) means all results
orders_src <- dbFetch(rs, n=-1) 
# close connection
dbDisconnect(con)
# Stop timer
proc.time() - ptm

# Refine and Categorize
registered_src$status<-NULL
verified_src$CITY<-NULL
verified_src$PREFECTURE<-NULL
orders_src$COMMISSION<-NULL
orders_src$ORDER_VALUE<-NULL
orders_src$PREFECTURE<-NULL
orders_src$CITY<-NULL

registered_src$cat<-""
verified_src$cat<-""
orders_src$cat<-""

# Android
registered_src$cat[registered_src$SOURCE == "Android"]<-"android"
verified_src$cat[verified_src$SOURCE == "Android"]<-"android"
orders_src$cat[orders_src$SOURCE == "Android"]<-"android"


# iOS
registered_src$cat[registered_src$SOURCE == "IOS"]<-"ios"
verified_src$cat[verified_src$SOURCE == "IOS"]<-"ios"
orders_src$cat[orders_src$SOURCE == "IOS"]<-"ios"
# Organic
## Google
registered_src$cat[grep("google", registered_src$SOURCE , ignore.case=FALSE, fixed=FALSE)]<-"organic"
verified_src$cat[grep("google", verified_src$SOURCE , ignore.case=FALSE, fixed=FALSE)]<-"organic"
orders_src$cat[grep("google", orders_src$SOURCE , ignore.case=FALSE, fixed=FALSE)]<-"organic"
## Yahoo
registered_src$cat[grep("yahoo", registered_src$SOURCE , ignore.case=FALSE, fixed=FALSE)]<-"organic"
verified_src$cat[grep("yahoo", verified_src$SOURCE , ignore.case=FALSE, fixed=FALSE)]<-"organic"
orders_src$cat[grep("yahoo", orders_src$SOURCE , ignore.case=FALSE, fixed=FALSE)]<-"organic"
## Bing
registered_src$cat[grep("bing", registered_src$SOURCE , ignore.case=FALSE, fixed=FALSE)]<-"organic"
verified_src$cat[grep("bing", verified_src$SOURCE , ignore.case=FALSE, fixed=FALSE)]<-"organic"
orders_src$cat[grep("bing", orders_src$SOURCE , ignore.case=FALSE, fixed=FALSE)]<-"organic"
## Search
registered_src$cat[grep("search", registered_src$SOURCE , ignore.case=FALSE, fixed=FALSE)]<-"organic"
verified_src$cat[grep("search", verified_src$SOURCE , ignore.case=FALSE, fixed=FALSE)]<-"organic"
orders_src$cat[grep("search", orders_src$SOURCE , ignore.case=FALSE, fixed=FALSE)]<-"organic"

# Newsletter
registered_src$cat[grep("newsletter", registered_src$SOURCE , ignore.case=FALSE, fixed=FALSE)]<-"newsletter"
verified_src$cat[grep("newsletter", verified_src$SOURCE , ignore.case=FALSE, fixed=FALSE)]<-"newsletter"
orders_src$cat[grep("newsletter", orders_src$SOURCE , ignore.case=FALSE, fixed=FALSE)]<-"newsletter"

# Facebook
registered_src$cat[grep("facebook", registered_src$SOURCE , ignore.case=FALSE, fixed=FALSE)]<-"facebook"
verified_src$cat[grep("facebook", verified_src$SOURCE , ignore.case=FALSE, fixed=FALSE)]<-"facebook"
orders_src$cat[grep("facebook", orders_src$SOURCE , ignore.case=FALSE, fixed=FALSE)]<-"facebook"

# Affiliate
registered_src$cat[grep("linkwise", registered_src$SOURCE , ignore.case=FALSE, fixed=FALSE)]<-"affiliate"
verified_src$cat[grep("linkwise", verified_src$SOURCE , ignore.case=FALSE, fixed=FALSE)]<-"affiliate"
orders_src$cat[grep("linkwise", orders_src$SOURCE , ignore.case=FALSE, fixed=FALSE)]<-"affiliate"

# Direct 
registered_src$cat[registered_src$cat == ""]<-"direct"
verified_src$cat[verified_src$cat == ""]<-"direct"
orders_src$cat[orders_src$cat == ""]<-"direct"

# Adwords
registered_src$cat[grep("google\\|cpc", registered_src$SOURCE , ignore.case=FALSE, fixed=TRUE)]<-"adwords"
verified_src$cat[grep("google\\|cpc", verified_src$SOURCE , ignore.case=FALSE, fixed=TRUE)]<-"adwords"
orders_src$cat[grep("google\\|cpc", orders_src$SOURCE , ignore.case=FALSE, fixed=TRUE)]<-"adwords"


library(plyr)
reg<-ddply(registered_src,("cat"), summarize, registration=sum(REGISTERED_USERS))
ver<-ddply(verified_src,("cat"), summarize, verifications=sum(VERIFIED_USERS))
ord<-ddply(orders_src,("cat"), summarize, orders=sum(VERIFIED_ORDERS))

# Stop timer
proc.time() - ptm