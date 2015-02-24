# # Load environment
# load("C:/Users/tantonakis/Google Drive/Scripts/AnalyticsProj/cdgr_mysql/Orders_form_2014.RData")
# # Load package
# library(RMySQL)
# library(xlsx)
# library(lubridate)
# library(zoo)
# library(plyr)
# library(dplyr)
# # Set timer
ptm <- proc.time()
# Establish connection
con <- dbConnect(RMySQL::MySQL(), host = '172.20.0.1', port = 3307, dbname = "beta",
                 user = "tantonakis", password = "2secret4usAll!")
# Send query
rs <- dbSendQuery(con,"
                  
SELECT `restaurant_master`.`restaurant_id`,
                `prefecture_detail`.`prefecture_name`

                
        	FROM `restaurant_master`
		LEFT JOIN `city_master`
		ON (`restaurant_master`.`restaurant_city_id` = `city_master`.`city_id`)
		LEFT JOIN `city_detail`
		ON (`city_detail`.`language_id` = 1 AND `city_master`.`city_id` = `city_detail`.`city_id`)
		LEFT JOIN `prefecture_detail`
		ON (`prefecture_detail`.`language_id` = 1 AND `city_master`.`prefecture_id` = `prefecture_detail`.`prefecture_id`)
                
                  
                  ")
# Fetch query results (n=-1) means all results
rest_pref_map <- dbFetch(rs, n=-1) 
# close connection
dbDisconnect(con)

final<-merge(orders, rest_pref_map, by.x='restaurant_id', by.y='restaurant_id')
final$restaurant_name<-NULL
final$status<-NULL
final$restaurant_id<-NULL
final$city_id<-NULL
final$prefecture_id<-NULL
final<- filter(final, (prefecture_name == "Larissa" | prefecture_name == "Volos"))
final$moy<-paste(year(final$order_date), month(final$order_date), sep="-")
final$month<-month(final$order_date)
final$year<-year(final$order_date)

table(final$year, final$month, final$prefecture_name)

ddply(final, c("prefecture_name", "year", "month"),summarize, revenue = sum(revenue), commission = sum(commission))

# Stop timer
proc.time() - ptm
