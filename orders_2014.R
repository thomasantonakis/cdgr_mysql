#Load environment
load("C:/Users/tantonakis/Google Drive/Scripts/AnalyticsProj/cdgr_mysql/Orders_form_2014.RData")

# Load package
library(RMySQL)
library(xlsx)
library(plyr)
library(dplyr)
# Set timer
ptm <- proc.time()

# Read new data every day or week and incorporate to orders

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
                  round(`order_master`.`order_amt`, 2) as revenue,
                  round(`order_master`.`order_commission`, 2) as commission 
                  
                  
                  
                  FROM  `order_master` 
                  JOIN `restaurant_master`
                  USING (`restaurant_id`)
                  JOIN `user_master`
                  ON `order_master`.`user_id` = `user_master`.`user_id`
                  JOIN `restaurant_detail`
                  ON (`restaurant_detail`.`restaurant_id` = `restaurant_master`.`restaurant_id` AND `restaurant_detail`.`language_id` = 1)
                  
                  
                  WHERE  `order_master`.`i_date` >= UNIX_TIMESTAMP('2015-02-26')
                  and ((`order_master`.`is_deleted` = 'N') and ((`order_master`.`status` = 'VERIFIED') or (`order_master`.`status` = 'REJECTED')))
                  and  `restaurant_detail`.`language_id` = 1
                  
                  
                  order by revenue DESC
                  
                  
                  ")
# Fetch query results (n=-1) means all results
new_orders <- dbFetch(rs, n=-1) 
# close connection
dbDisconnect(con)
# Stop timer
proc.time() - ptm

orders<-rbind(orders, new_orders)
orders<-distinct(orders)


# Recalculate ord_user, lod, ord_user
ord_user<-as.data.frame(table(orders$user_id))
names(ord_user)<-c("user_id", "orders")
ord_user$user_id<-as.numeric(as.character(ord_user$user_id))
ord_user<-arrange(ord_user, desc(orders))
gc()
lod<-ddply(orders,("user_id"), summarize, last_ord_date=max(as.Date(order_date)))
lod$user_id<-as.numeric(lod$user_id)

ord_user<-merge(ord_user, lod)


rm(lod, new_orders)
gc()
# Segmentation for how long since last order
ord_user$days_idle<-as.Date(as.character(Sys.time(),format="%Y-%m-%d"))-as.Date(as.character(ord_user$last_ord_date,format="%Y-%m-%d"))
group <- cut(as.numeric(ord_user$days_idle), c(0,30, 60, 90, 120, 150, 180, Inf), right=FALSE)
names(group)<-c("Less than 30 days", "1-2 months", "2-3 months", "3-4 months", "4-5 months", "5-6 months", "More than 6 months")
ord_user$grp_idle <- group
barplot(table(ord_user$grp_idle), main="How long ago was your last order?", xlab="Days",
        names.arg=c("Less than 30 days", "1-2 months", "2-3 months", "3-4 months", "4-5 months", "5-6 months", "More than 6 months"))

# How to exclude oldest (first) order
wo_oldest<-arrange(orders, user_id, order_date)
wo_oldest$index<- FALSE
wo_oldest$index[2:nrow(wo_oldest)]<-wo_oldest$user_id[2:nrow(wo_oldest)] == wo_oldest$user_id[1:(nrow(wo_oldest)-1)]
wo_oldest<-wo_oldest[wo_oldest$index,]

# Recalculate ord_user, lod, ord_user
ord_user_wo_oldest<-as.data.frame(table(wo_oldest$user_id))
names(ord_user_wo_oldest)<-c("user_id", "orders")
ord_user_wo_oldest$user_id<-as.numeric(as.character(ord_user_wo_oldest$user_id))
ord_user_wo_oldest<-arrange(ord_user_wo_oldest, desc(orders))
gc()
lod_wo_oldest<-ddply(wo_oldest,("user_id"), summarize, last_ord_date=max(as.Date(order_date)))
lod_wo_oldest$user_id<-as.numeric(lod_wo_oldest$user_id)

ord_user_wo_oldest<-merge(ord_user_wo_oldest, lod_wo_oldest)
rm(lod_wo_oldest)
gc()

# Segmentation for how long since last order
ord_user_wo_oldest$days_idle<-as.Date(as.character(Sys.time(),format="%Y-%m-%d"))-as.Date(as.character(ord_user_wo_oldest$last_ord_date,format="%Y-%m-%d"))
group <- cut(as.numeric(ord_user_wo_oldest$days_idle), c(0,30, 60, 90, 120, 150, 180, Inf), right=FALSE)
names(group)<-c("Less than 30 days", "1-2 months", "2-3 months", "3-4 months", "4-5 months", "5-6 months", "More than 6 months")
ord_user_wo_oldest$grp_idle <- group
barplot(table(ord_user_wo_oldest$grp_idle), main="How long ago was your last order?", xlab="Days",
        names.arg=c("Less than 30 days", "1-2 months", "2-3 months", "3-4 months", "4-5 months", "5-6 months", "More than 6 months"))


rm(group)
# Save workspace environment
save.image("C:/Users/tantonakis/Google Drive/Scripts/AnalyticsProj/cdgr_mysql/Orders_form_2014.RData")
# Print environment size
print(paste('R is using', memory.size(), 'MB out of limit', memory.limit(), 'MB'))
# Stop timer
proc.time() - ptm
