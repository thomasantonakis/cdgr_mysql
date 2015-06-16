#################################################################
# android
#################################################################
# Load package
library(RMySQL)
# Set timer
ptm <- proc.time()
# Establish connection
con <- dbConnect(RMySQL::MySQL(), host = '172.20.0.1', port = 3307, dbname = "beta",
                 user = "tantonakis", password = "2secret4usAll!")
# Send query
rs <- dbSendQuery(con,"
                  
                  SELECT DATE(FROM_UNIXTIME(`user_master`.`i_date`))AS DATE,
                  
                  count(distinct`user_master`.`user_id`) as users,
                  `user_master`.`status` as status
                  
                  FROM `user_master`
                  
                  WHERE `user_master`.`i_date` >= UNIX_TIMESTAMP('2015-05-12')
                  AND `user_master`.`i_date` < UNIX_TIMESTAMP('2015-07-01')
                  AND `user_master`.`is_deleted` = 'N'
                  AND `user_master`.`referal_source`  LIKE '%Android|quizdom%'
                  
                  GROUP BY DATE, status
                  ORDER BY status,DATE
                  
                  ")
# Fetch query results (n=-1) means all results
andd1 <- dbFetch(rs, n=-1) 
# close connection
dbDisconnect(con)
# Stop timer
proc.time() - ptm

# andd1<-andd1[d1$SOURCE=='Android|quizdom|cpr|quizdom_registration|campaign_id:574856',1:2]
# andd1<-andd1[grep("Android|quizdom", d1$SOURCE),1:2]
# table(andd1$status)

# library(plyr)
# ddply(andd1,.(DATE),summarize,asda = count(status))



# Load package
library(RMySQL)

# Establish connection
con <- dbConnect(RMySQL::MySQL(), host = '172.20.0.1', port = 3307, dbname = "beta",
                 user = "tantonakis", password = "2secret4usAll!")
# Send query
rs <- dbSendQuery(con,"
                  
                  SELECT `user_master`.`user_id`, `user_master`.`status`, `user_master`.`referal_source` AS SOURCE,
                  DATE(FROM_UNIXTIME(`user_master`.`i_date`))AS date,
                  DATE(FROM_UNIXTIME(`user_master`.`verification_date`))AS verified,
                  `user_master`.`mobile`,`user_master`.`email_id`, `user_master`.`sms`
                  
                  FROM `user_master`
                  
                  WHERE `user_master`.`i_date` >= UNIX_TIMESTAMP('2015-05-12')
                  AND `user_master`.`i_date` < UNIX_TIMESTAMP('2015-07-01')
                  AND `user_master`.`is_deleted` = 'N'
                  
                  
                  ")
# Fetch query results (n=-1) means all results
andd2 <- dbFetch(rs, n=-1) 
# close connection
dbDisconnect(con)
# Stop timer
proc.time() - ptm

# andd2<-andd2[andd2$SOURCE=='Android|quizdom|cpr|quizdom_registration|campaign_id:574856',1:2]
andd2<-andd2[grep("Android\\|quizdom", andd2$SOURCE),]
andd2$date<-as.Date(andd2$date)
andd2$verified<-as.Date(andd2$verified)

# andd2[andd2$status=="VERIFIED",]
# View(`andd1`)

# Extract usernames from referal source

# Create function to do that
substrRight <- function(x, n){
        substr(x, nchar(x)-n+1, nchar(x))
}
# 
quizdom_verified_and<-as.data.frame(substrRight(andd2[andd2$status=="VERIFIED",3], nchar(andd2[andd2$status=="VERIFIED",3])
                                                -rev(gregexpr("\\:",  andd2[andd2$status=="VERIFIED",3])[[1]])[1]))
# Create data-frame in order to be saved in file
quizdom_verified_and<-cbind(andd2[andd2$status=="VERIFIED",c(1,4:8)],quizdom_verified_and )
# Correct names
names(quizdom_verified_and)<-c("user_id", "date","verified", "mobile", "email", "sms", "username")
# Sort the dataframe so as to know what has already been given out
quizdom_verified_and <- quizdom_verified_and[order(quizdom_verified_and$verified),] 

# Registrations
quizdom_reg_and<-as.data.frame(substrRight(andd2[andd2$status!="VERIFIED",3], nchar(andd2[andd2$status!="VERIFIED",3])
                                           -rev(gregexpr("\\:",  andd2[andd2$status!="VERIFIED",3])[[1]])[1]))
# Create data-frame in order to be saved in file
quizdom_reg_and<-cbind(andd2[andd2$status!="VERIFIED",c(1,4:8)],quizdom_reg_and )
# Correct names
names(quizdom_reg_and)<-c("user_id", "date","verified", "mobile", "email", "sms", "username")
# Sort the dataframe so as to know what has already been given out
quizdom_reg_and <- quizdom_reg_and[order(quizdom_reg_and$verified),] 
quizdom_reg_and$verified<-NULL
quizdom_reg_and$device<-"android"

# table(andd2$status)


#################################################################
# iOS
#################################################################
# Load package
library(RMySQL)

# Establish connection
con <- dbConnect(RMySQL::MySQL(), host = '172.20.0.1', port = 3307, dbname = "beta",
                 user = "tantonakis", password = "2secret4usAll!")
# Send query
rs <- dbSendQuery(con,"
                  
                  SELECT DATE(FROM_UNIXTIME(`user_master`.`i_date`))AS DATE,
                  
                  count(distinct`user_master`.`user_id`) as users,
                  `user_master`.`status` as status
                  
                  FROM `user_master`
                  
                  WHERE `user_master`.`i_date` >= UNIX_TIMESTAMP('2015-05-21')
                  AND `user_master`.`i_date` < UNIX_TIMESTAMP('2015-07-01')
                  AND `user_master`.`is_deleted` = 'N'
                  AND `user_master`.`referal_source`  LIKE '%IOS|quizdom%'
                  
                  GROUP BY DATE, status
                  ORDER BY status,DATE
                  
                  ")
# Fetch query results (n=-1) means all results
iosd1 <- dbFetch(rs, n=-1) 
# close connection
dbDisconnect(con)
# Stop timer
proc.time() - ptm

# iosd1<-iosd1[iosd1$SOURCE=='IOS|quizdom|cpr|quizdom_registration|campaign_id:574856',1:2]
# iosd1<-iosd1[grep("IOS|quizdom", iosd1$SOURCE),1:2]
# table(iosd1$status)

# library(plyr)
# ddply(iosd1,.(DATE),summarize,asda = count(status))



# Load package
library(RMySQL)

# Establish connection
con <- dbConnect(RMySQL::MySQL(), host = '172.20.0.1', port = 3307, dbname = "beta",
                 user = "tantonakis", password = "2secret4usAll!")
# Send query
rs <- dbSendQuery(con,"
                  
                  SELECT `user_master`.`user_id`, `user_master`.`status`, `user_master`.`referal_source` AS SOURCE,
                  DATE(FROM_UNIXTIME(`user_master`.`i_date`))AS date,
                  DATE(FROM_UNIXTIME(`user_master`.`verification_date`))AS verified,
                  `user_master`.`mobile`,`user_master`.`email_id`, `user_master`.`sms`
                  
                  FROM `user_master`
                  
                  WHERE `user_master`.`i_date` >= UNIX_TIMESTAMP('2015-05-21')
                  AND `user_master`.`i_date` < UNIX_TIMESTAMP('2015-07-01')
                  AND `user_master`.`is_deleted` = 'N'
                  
                  
                  ")
# Fetch query results (n=-1) means all results
iosd2 <- dbFetch(rs, n=-1) 
# close connection
dbDisconnect(con)
# Stop timer
proc.time() - ptm

# iosd2<-iosd2[iosd2$SOURCE=='IOS|quizdom|cpr|quizdom_registration|campaign_id:574856',1:2]
iosd2<-iosd2[grep("IOS\\|quizdom", iosd2$SOURCE),]
iosd2$date<-as.Date(iosd2$date)
iosd2$verified<-as.Date(iosd2$verified)

# iosd2[iosd2$status=="VERIFIED",]
# View(`iosd1`)

# Extract usernames from referal source

# # Create function to do that
# substrRight <- function(x, n){
#         substr(x, nchar(x)-n+1, nchar(x))
# }
# # 
quizdom_verified_ios<-as.data.frame(substrRight(iosd2[iosd2$status=="VERIFIED",3], nchar(iosd2[iosd2$status=="VERIFIED",3])
                                                -rev(gregexpr("\\:",  iosd2[iosd2$status=="VERIFIED",3])[[1]])[1]))
# Create data-frame in order to be saved in file
quizdom_verified_ios<-cbind(iosd2[iosd2$status=="VERIFIED",c(1,4:8)],quizdom_verified_ios )
# Correct names
names(quizdom_verified_ios)<-c("user_id", "date","verified", "mobile", "email", "sms", "username")
# Sort the dataframe so as to know what has already been given out
quizdom_verified_ios <- quizdom_verified_ios[order(quizdom_verified_ios$verified),] 

# Registrations
quizdom_reg_ios<-as.data.frame(substrRight(iosd2[iosd2$status!="VERIFIED",3], nchar(iosd2[iosd2$status!="VERIFIED",3])
                                           -rev(gregexpr("\\:",  iosd2[iosd2$status!="VERIFIED",3])[[1]])[1]))
# Create data-frame in order to be saved in file
quizdom_reg_ios<-cbind(iosd2[iosd2$status!="VERIFIED",c(1,4:8)],quizdom_reg_ios )
# Correct names
names(quizdom_reg_ios)<-c("user_id", "date","verified", "mobile", "email", "sms", "username")
# Sort the dataframe so as to know what has already been given out
quizdom_reg_ios <- quizdom_reg_ios[order(quizdom_reg_ios$verified),] 
quizdom_reg_ios$verified<-NULL
quizdom_reg_ios$device<-"ios"
# table(iosd2$status)



#############################################################
# Orders total
#############################################################

# Load package
library(RMySQL)

# Establish connection
con <- dbConnect(RMySQL::MySQL(), host = '172.20.0.1', port = 3307, dbname = "beta",
                 user = "tantonakis", password = "2secret4usAll!")
# Send query
rs <- dbSendQuery(con,"
                  
                  SELECT `user_master`.`user_id`, `user_master`.`status`, `user_master`.`referal_source` AS SOURCE,
                  DATE(FROM_UNIXTIME(`user_master`.`i_date`))AS date,
                  DATE(FROM_UNIXTIME(`user_master`.`verification_date`))AS verified,
                  `user_master`.`mobile`,`user_master`.`email_id`, 
                  `order_master`.`order_id`,
                  `order_master`.`order_referal`
                  
                  FROM `user_master`
                  JOIN `order_master`
                  USING (`user_id`)
                  
                  WHERE `user_master`.`i_date` >= UNIX_TIMESTAMP('2015-05-12')
                  AND `user_master`.`i_date` < UNIX_TIMESTAMP('2015-07-01')
                  AND `user_master`.`is_deleted` = 'N'
                  AND `user_master`.`referal_source`  LIKE '%quizdom%'

                  ")
# Fetch query results (n=-1) means all results
orders_total <- dbFetch(rs, n=-1) 
# close connection
dbDisconnect(con)
# Stop timer
proc.time() - ptm

#############################################################
# Orders Summary
#############################################################

# Load package
library(RMySQL)

# Establish connection
con <- dbConnect(RMySQL::MySQL(), host = '172.20.0.1', port = 3307, dbname = "beta",
                 user = "tantonakis", password = "2secret4usAll!")
# Send query
rs <- dbSendQuery(con,"
                  
                  SELECT `user_master`.`user_id`, `user_master`.`status`, `user_master`.`referal_source` AS SOURCE,
                  DATE(FROM_UNIXTIME(`user_master`.`i_date`))AS date,
                  DATE(FROM_UNIXTIME(`user_master`.`verification_date`))AS verified,
                  `user_master`.`mobile`,`user_master`.`email_id`, 
                  count(distinct `order_master`.`order_id`) as order_count
                  
                  
                  FROM `user_master`
                  JOIN `order_master`
                  USING (`user_id`)
                  
                  WHERE `user_master`.`i_date` >= UNIX_TIMESTAMP('2015-05-12')
                  AND `user_master`.`i_date` < UNIX_TIMESTAMP('2015-07-01')
                  AND `user_master`.`is_deleted` = 'N'
                  AND `user_master`.`referal_source`  LIKE '%quizdom%'

                  GROUP BY user_id

                  ")
# Fetch query results (n=-1) means all results
orders_summary <- dbFetch(rs, n=-1) 
# close connection
dbDisconnect(con)
# Stop timer
proc.time() - ptm


#############################################################
# Orders Check with tx
#############################################################

# Load package
library(RMySQL)

# Establish connection
con <- dbConnect(RMySQL::MySQL(), host = '172.20.0.1', port = 3307, dbname = "beta",
                 user = "tantonakis", password = "2secret4usAll!")
# Send query
rs <- dbSendQuery(con,"
                  
                  SELECT `order_master`.`order_id` ,  
                  `order_master`.`order_referal`,
                  DATE(FROM_UNIXTIME(`order_master`.`i_date`))AS ord_date,
                  DATE(FROM_UNIXTIME(`user_master`.`verification_date`))AS ver_date,
                  `user_master`.`user_id`,
                  `user_master`.`referal_source`
                                    
                  FROM  `order_master`
                  JOIN `user_master`
                  USING (`user_id`)
                  
                  WHERE `order_master`.`i_date` >= UNIX_TIMESTAMP('2015-05-12')
                  AND `order_master`.`i_date` < UNIX_TIMESTAMP('2015-07-01')
                  AND ((`order_master`.`is_deleted` = 'N') and ((`order_master`.`status` = 'VERIFIED') or (`order_master`.`status` = 'REJECTED')))
                  AND `order_master`.`order_referal`  LIKE '%quizdom%'
                  

                  ")
# Fetch query results (n=-1) means all results
orders_quizdom <- dbFetch(rs, n=-1) 
# close connection
dbDisconnect(con)

# Prepare for export in summary
andd1$cat<-"android"
iosd1$cat<-"ios"
export<-rbind(andd1, iosd1)
export$DATE<-as.Date(export$DATE)
library(xlsx)
#Export the working file
setwd("C:/Users/tantonakis/Google Drive/MARKETING MIX/SYNERGIES (1)/Quizdom")
write.xlsx(export, file="quizdom.xlsx", row.names = FALSE)
setwd("C:/Users/tantonakis/Google Drive/Scripts/AnalyticsProj/cdgr_mysql")
# Stop timer
proc.time() - ptm


# Prepare for cohort and lag days working file
andd2$cat<-"android"
iosd2$cat<-"ios"
reg_lag<-rbind(andd2, iosd2)
reg_lag$lag<-NA
# Calculate lag days for verified
reg_lag$lag[reg_lag$status=='VERIFIED']<-reg_lag$verified[reg_lag$status=='VERIFIED'] - reg_lag$date[reg_lag$status=='VERIFIED']
# Calculate idle days for just registered
reg_lag$lag[reg_lag$status!='VERIFIED']<-as.Date(Sys.Date( ) )- reg_lag$date[reg_lag$status!='VERIFIED']
# Create data frames for copy paste
ver_cohort<-as.data.frame(table(reg_lag$lag[reg_lag$status=='VERIFIED'])) 
reg_cohort<-as.data.frame(table(reg_lag$lag[reg_lag$status!='VERIFIED'])) 
row.names(reg_cohort)<-NULL
names(reg_cohort)<-c("days", "users")
row.names(ver_cohort)<-NULL
names(ver_cohort)<-c("days", "users")

# Prepare for export for eligible customers for sms
# Merge the two registered dataframes
export<-rbind(quizdom_reg_and, quizdom_reg_ios)
# Exclude users who do not want sms
export<-export[export$sms ==1,]
# Include only mobiles that start with 69
export<-export[grepl("^69", export$mobile),]
# Convert mobile to numeric
export$mobile<-as.numeric(export$mobile)
# So as to exclude those with strange characters
export<-export[!is.na(export$mobile),]
# Exclude quizdom emails! :-)
export<-export[!grepl("quizdom", export$email),]

# #Export the eligible for sms
# setwd("C:/Users/tantonakis/Google Drive/MARKETING MIX/SYNERGIES (1)/Quizdom")
# write.csv2(export[,c(1.3)], file = "quizdom_reg_sms.csv",row.names = FALSE)
# # write.xlsx(export, file="quizdom2.xlsx", row.names = FALSE)
# setwd("C:/Users/tantonakis/Google Drive/Scripts/AnalyticsProj/cdgr_mysql")

#Check with Export
table(andd2$status)
table(iosd2$status)
# Stop timer
proc.time() - ptm