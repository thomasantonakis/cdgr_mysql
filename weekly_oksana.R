ptm <- proc.time()
#########################################
# Verified Users From mySQL
#########################################

# Load previously exported csv
report<-read.csv('wmf.csv', stringsAsFactors = FALSE)
report$date<-as.Date(report$date)

# Load package
library(RMySQL)
# Set timer
proc.time() - ptm
# Establish connection
con <- dbConnect(RMySQL::MySQL(), host = '172.20.0.1', port = 3307, dbname = "beta",
                 user = "tantonakis", password = "2secret4usAll!")
# Send query
rs <- dbSendQuery(con,"
                  
                  SELECT COUNT(*) AS VERIFIED_USERS,
                  `user_master`.`referal_source` AS SOURCE,
                  date(FROM_UNIXTIME(`user_master`.`verification_date`)) as date
                  
                  FROM `user_master`
                  
                  
                  WHERE `user_master`.`verification_date` >= UNIX_TIMESTAMP('2015-01-01')
                  
                  AND `user_master`.`status` = 'VERIFIED'
                  AND `user_master`.`is_deleted` = 'N'
                  GROUP BY `user_master`.`referal_source`, date(FROM_UNIXTIME(`user_master`.`verification_date`)) 
                  
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
                  
                  SELECT COUNT(*) AS REGISTERED_USERS,  `user_master`.`referal_source` AS SOURCE,
                  date(FROM_UNIXTIME(`user_master`.`i_date`)) as date

                  FROM `user_master`
                  WHERE `user_master`.`i_date` >= UNIX_TIMESTAMP('2015-01-01')
                  
                  AND `user_master`.`is_deleted` = 'N'
                  GROUP BY `user_master`.`referal_source`, date(FROM_UNIXTIME(`user_master`.`i_date`))
                  
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
                  `order_master`.`order_referal` AS SOURCE ,
                  date(FROM_UNIXTIME(`order_master`.`i_date`)) as date
                  
                  FROM `order_master`
                  
                  WHERE `order_master`.`i_date` >= UNIX_TIMESTAMP('2015-01-01')
                  
                  AND `order_master`.`status` IN ('VERIFIED', 'REJECTED')
                  AND `order_master`.`is_deleted` = 'N'
                  GROUP BY  `order_master`.`order_referal`, date(FROM_UNIXTIME(`order_master`.`i_date`))
                  
                  ")
# Fetch query results (n=-1) means all results
orders_src <- dbFetch(rs, n=-1) 
# close connection
dbDisconnect(con)
# Stop timer
proc.time() - ptm



#########################################
# Active Users From mySQL
#########################################

# Establish connection
con <- dbConnect(RMySQL::MySQL(), host = '172.20.0.1', port = 3307, dbname = "beta",
                 user = "tantonakis", password = "2secret4usAll!")
# Send query
rs <- dbSendQuery(con,"


SELECT count(*) AS users,
                `user_master`.`referal_source` AS SOURCE,
                date(FROM_UNIXTIME(`user_master`.`verification_date`)) as date

FROM `user_master`

WHERE `user_master`.`verification_date` >= UNIX_TIMESTAMP('2015-01-01')

AND `user_master`.`verification_date` - `user_master`.`i_date` <= 86400
AND `user_master`.`status` = 'VERIFIED'
AND `user_master`.`is_deleted` = 'N'

GROUP BY source, date(FROM_UNIXTIME(`user_master`.`verification_date`))

                  
                  ")
# Fetch query results (n=-1) means all results
active_users <- dbFetch(rs, n=-1) 
# close connection
dbDisconnect(con)
# Stop timer
proc.time() - ptm

# report<-as.data.frame(as.Date(unique(orders_src$date[orders_src$date>max(report$date)])))
# names(report)<-"date"
# report$R_SEM_Brand<-0
# report$R_SEM_Non_Brand<-0
# report$R_SEM_Facebook<-0
# report$R_SEM_Youtube<-0
# report$R_SEM_Remarketing<-0
# report$R_SEM_ios<-0
# report$R_SEM_android<-0
# report$R_OTH_android<-0
# report$V_SEM_Brand<-0
# report$V_SEM_Non_Brand<-0
# report$V_SEM_Facebook<-0
# report$V_SEM_Youtube<-0
# report$V_SEM_Remarketing<-0
# report$V_SEM_ios<-0
# report$V_SEM_android<-0
# report$V_OTH_android<-0
# report$O_SEM_Brand<-0
# report$O_SEM_Non_Brand<-0
# report$O_SEM_Facebook<-0
# report$O_SEM_Youtube<-0
# report$O_SEM_Remarketing<-0
# report$O_SEM_ios<-0
# report$O_SEM_android<-0
# report$O_OTH_android<-0
# report$AU_SEM_Brand<-0
# report$AU_SEM_Non_Brand<-0
# report$AU_SEM_Facebook<-0
# report$AU_SEM_Youtube<-0
# report$AU_SEM_Remarketing<-0
# report$AU_SEM_ios<-0
# report$AU_SEM_android<-0
# report$AU_OTH_android<-0
# report$R_tot<-0
# report$V_tot<-0
# report$O_tot<-0
# report$AU_tot<-0
# report$R_ios_tot<-0
# report$V_ios_tot<-0
# report$O_ios_tot<-0
# report$AU_ios_tot<-0
# report$R_and_tot<-0
# report$V_and_tot<-0
# report$O_and_tot<-0
# report$AU_and_tot<-0

report<-report[-nrow(report),]
tbadded<-report[1:length(unique(orders_src$date[orders_src$date>max(report$date)])),]
tbadded$date<-as.Date(unique(orders_src$date[orders_src$date>max(report$date)]))

for (i in 1:nrow(tbadded)){
        tbadded$R_SEM_Brand[i]<-sum(registered_src$REGISTERED_USERS[registered_src$date==tbadded[i,1] & 
                                                                registered_src$SOURCE =='google|cpc|Brand']) 
        tbadded$R_SEM_Non_Brand[i]<-sum(registered_src$REGISTERED_USERS[registered_src$date==tbadded[i,1] & 
                                                            grepl("^google\\|cpc", registered_src$SOURCE , ignore.case=FALSE, fixed=FALSE)]) -tbadded$R_SEM_Brand[i]
        tbadded$R_SEM_Facebook[i]<-sum(registered_src$REGISTERED_USERS[registered_src$date==tbadded[i,1] & 
                                                            grepl("^facebook", registered_src$SOURCE , ignore.case=FALSE, fixed=FALSE)]) 
        tbadded$R_SEM_Youtube[i]<-sum(registered_src$REGISTERED_USERS[registered_src$date==tbadded[i,1] & 
                                                                 grepl("youtube", registered_src$SOURCE , ignore.case=FALSE, fixed=FALSE)]) 
        tbadded$R_SEM_Remarketing[i]<-sum(registered_src$REGISTERED_USERS[registered_src$date==tbadded[i,1] & 
                                                                 grepl("remarketing", registered_src$SOURCE , ignore.case=FALSE, fixed=FALSE)]) 
        tbadded$R_SEM_ios[i]<-sum(registered_src$REGISTERED_USERS[registered_src$date==tbadded[i,1] & 
                                                                registered_src$SOURCE =='IOS']) 
        tbadded$R_SEM_android[i]<-sum(registered_src$REGISTERED_USERS[registered_src$date==tbadded[i,1] & (
                                                                 grepl("Android\\|google\\|cpc", registered_src$SOURCE , ignore.case=FALSE, fixed=FALSE)| 
                                                                 grepl("Android\\|google\\|display", registered_src$SOURCE , ignore.case=FALSE, fixed=FALSE)| 
                                                                 grepl("Android\\|google\\|mobdisplay", registered_src$SOURCE , ignore.case=FALSE, fixed=FALSE)
                                                                 )]) 
        tbadded$R_OTH_android[i]<-sum(registered_src$REGISTERED_USERS[registered_src$date==tbadded[i,1] & (
                grepl("Android\\|facebook", registered_src$SOURCE , ignore.case=FALSE, fixed=FALSE)| 
                        grepl("Android\\|quizdom", registered_src$SOURCE , ignore.case=FALSE, fixed=FALSE)
        )]) 
        tbadded$V_SEM_Brand[i]<-sum(verified_src$VERIFIED_USERS[verified_src$date==tbadded[i,1] & 
                                                              verified_src$SOURCE =='google|cpc|Brand']) 
        tbadded$V_SEM_Non_Brand[i]<-sum(verified_src$VERIFIED_USERS[verified_src$date==tbadded[i,1] & 
                                                            grepl("^google\\|cpc", verified_src$SOURCE , ignore.case=FALSE, fixed=FALSE)]) -tbadded$V_SEM_Brand[i]
        tbadded$V_SEM_Facebook[i]<-sum(verified_src$VERIFIED_USERS[verified_src$date==tbadded[i,1] & 
                                                            grepl("^facebook", verified_src$SOURCE , ignore.case=FALSE, fixed=FALSE)]) 
        tbadded$V_SEM_Youtube[i]<-sum(verified_src$VERIFIED_USERS[verified_src$date==tbadded[i,1] & 
                                                             grepl("youtube", verified_src$SOURCE , ignore.case=FALSE, fixed=FALSE)]) 
        tbadded$V_SEM_Remarketing[i]<-sum(verified_src$VERIFIED_USERS[verified_src$date==tbadded[i,1] & 
                                                             grepl("remarketing", verified_src$SOURCE , ignore.case=FALSE, fixed=FALSE)]) 
        tbadded$V_SEM_ios[i]<-sum(verified_src$VERIFIED_USERS[verified_src$date==tbadded[i,1] & 
                                                               verified_src$SOURCE =='IOS']) 
        tbadded$V_SEM_android[i]<-sum(verified_src$VERIFIED_USERS[verified_src$date==tbadded[i,1] & (
                grepl("Android\\|google\\|cpc", verified_src$SOURCE , ignore.case=FALSE, fixed=FALSE)| 
                        grepl("Android\\|google\\|display", verified_src$SOURCE , ignore.case=FALSE, fixed=FALSE)| 
                        grepl("Android\\|google\\|mobdisplay", verified_src$SOURCE , ignore.case=FALSE, fixed=FALSE)
        )]) 
        tbadded$V_OTH_android[i]<-sum(verified_src$VERIFIED_USERS[verified_src$date==tbadded[i,1] & (
                grepl("Android\\|facebook", verified_src$SOURCE , ignore.case=FALSE, fixed=FALSE)| 
                        grepl("Android\\|quizdom", verified_src$SOURCE , ignore.case=FALSE, fixed=FALSE)
        )]) 
        tbadded$O_SEM_Brand[i]<-sum(orders_src$VERIFIED_ORDERS[orders_src$date==tbadded[i,1] & 
                                                            orders_src$SOURCE =='google|cpc|Brand']) 
        tbadded$O_SEM_Non_Brand[i]<-sum(orders_src$VERIFIED_ORDERS[orders_src$date==tbadded[i,1] & 
                                                            grepl("^google\\|cpc", orders_src$SOURCE , ignore.case=FALSE, fixed=FALSE)]) - tbadded$O_SEM_Brand[i]
        tbadded$O_SEM_Facebook[i]<-sum(orders_src$VERIFIED_ORDERS[orders_src$date==tbadded[i,1] & 
                                                            grepl("^facebook", orders_src$SOURCE , ignore.case=FALSE, fixed=FALSE)]) 
        tbadded$O_SEM_Youtube[i]<-sum(orders_src$VERIFIED_ORDERS[orders_src$date==tbadded[i,1] & 
                                                             grepl("youtube", orders_src$SOURCE , ignore.case=FALSE, fixed=FALSE)]) 
        tbadded$O_SEM_Remarketing[i]<-sum(orders_src$VERIFIED_ORDERS[orders_src$date==tbadded[i,1] & 
                                                             grepl("remarketing", orders_src$SOURCE , ignore.case=FALSE, fixed=FALSE)]) 
        tbadded$O_SEM_ios[i]<-sum(orders_src$VERIFIED_ORDERS[orders_src$date==tbadded[i,1] & 
                                                             orders_src$SOURCE =='IOS']) 
        tbadded$O_SEM_android[i]<-sum(orders_src$VERIFIED_ORDERS[orders_src$date==tbadded[i,1]& (
                grepl("Android\\|google\\|cpc", orders_src$SOURCE , ignore.case=FALSE, fixed=FALSE)| 
                        grepl("Android\\|google\\|display", orders_src$SOURCE , ignore.case=FALSE, fixed=FALSE)| 
                        grepl("Android\\|google\\|mobdisplay", orders_src$SOURCE , ignore.case=FALSE, fixed=FALSE)
        )]) 
        tbadded$O_OTH_android[i]<-sum(orders_src$VERIFIED_ORDERS[orders_src$date==tbadded[i,1]& (
                grepl("Android\\|facebook", orders_src$SOURCE , ignore.case=FALSE, fixed=FALSE)| 
                       grepl("Android\\|quizdom", orders_src$SOURCE , ignore.case=FALSE, fixed=FALSE)
        )]) 
        tbadded$AU_SEM_Brand[i]<-sum(active_users$users[active_users$date==tbadded[i,1] & 
                                                               active_users$SOURCE =='google|cpc|Brand']) 
        tbadded$AU_SEM_Non_Brand[i]<-sum(active_users$users[active_users$date==tbadded[i,1] & 
                                                             grepl("^google\\|cpc", active_users$SOURCE , ignore.case=FALSE, fixed=FALSE)]) -tbadded$AU_SEM_Brand[i]
        tbadded$AU_SEM_Facebook[i]<-sum(active_users$users[active_users$date==tbadded[i,1] & 
                                                             grepl("^facebook", active_users$SOURCE , ignore.case=FALSE, fixed=FALSE)]) 
        tbadded$AU_SEM_Youtube[i]<-sum(active_users$users[active_users$date==tbadded[i,1] & 
                                                     grepl("youtube", active_users$SOURCE , ignore.case=FALSE, fixed=FALSE)]) 
        tbadded$AU_SEM_Remarketing[i]<-sum(active_users$users[active_users$date==tbadded[i,1] & 
                                                     grepl("remarketing", active_users$SOURCE , ignore.case=FALSE, fixed=FALSE)]) 
        tbadded$AU_SEM_ios[i]<-sum(active_users$users[active_users$date==tbadded[i,1] & 
                                                             active_users$SOURCE =='IOS']) 
        tbadded$AU_SEM_android[i]<-sum(active_users$users[active_users$date==tbadded[i,1]& (
                grepl("Android\\|google\\|cpc", active_users$SOURCE , ignore.case=FALSE, fixed=FALSE)| 
                        grepl("Android\\|google\\|display", active_users$SOURCE , ignore.case=FALSE, fixed=FALSE)| 
                        grepl("Android\\|google\\|mobdisplay", active_users$SOURCE , ignore.case=FALSE, fixed=FALSE)
        )]) 
        tbadded$AU_SEM_android[i]<-sum(active_users$users[active_users$date==tbadded[i,1]& (
                grepl("Android\\|facebook", active_users$SOURCE , ignore.case=FALSE, fixed=FALSE)| 
                        grepl("Android\\|quizdom", active_users$SOURCE , ignore.case=FALSE, fixed=FALSE)
        )]) 
        tbadded$R_tot[i]<-sum(registered_src$REGISTERED_USERS[registered_src$date==tbadded[i,1]])
        tbadded$V_tot[i]<-sum(verified_src$VERIFIED_USERS[verified_src$date==tbadded[i,1]])
        tbadded$O_tot[i]<-sum(orders_src$VERIFIED_ORDERS[orders_src$date==tbadded[i,1]])
        tbadded$AU_tot[i]<-sum(active_users$users[active_users$date==tbadded[i,1]])
        
        tbadded$R_ios_tot[i]<-sum(registered_src$REGISTERED_USERS[registered_src$date==tbadded[i,1] & 
                                         grepl("^IOS", registered_src$SOURCE , ignore.case=FALSE, fixed=FALSE)])
        tbadded$V_ios_tot[i]<-sum(verified_src$VERIFIED_USERS[verified_src$date==tbadded[i,1] & 
                                         grepl("^IOS", verified_src$SOURCE , ignore.case=FALSE, fixed=FALSE)])
        tbadded$O_ios_tot[i]<-sum(orders_src$VERIFIED_ORDERS[orders_src$date==tbadded[i,1] & 
                                         grepl("^IOS", orders_src$SOURCE , ignore.case=FALSE, fixed=FALSE)])
        tbadded$AU_ios_tot[i]<-sum(active_users$users[active_users$date==tbadded[i,1]& 
                                          grepl("^IOS", active_users$SOURCE , ignore.case=FALSE, fixed=FALSE)])
        
        tbadded$R_and_tot[i]<-sum(registered_src$REGISTERED_USERS[registered_src$date==tbadded[i,1] & 
                                         grepl("^Android", registered_src$SOURCE , ignore.case=FALSE, fixed=FALSE)])
        tbadded$V_and_tot[i]<-sum(verified_src$VERIFIED_USERS[verified_src$date==tbadded[i,1] & 
                                         grepl("^Android", verified_src$SOURCE , ignore.case=FALSE, fixed=FALSE)])
        tbadded$O_and_tot[i]<-sum(orders_src$VERIFIED_ORDERS[orders_src$date==tbadded[i,1] & 
                                         grepl("^Android", orders_src$SOURCE , ignore.case=FALSE, fixed=FALSE)])
        tbadded$AU_and_tot[i]<-sum(active_users$users[active_users$date==tbadded[i,1]& 
                                          grepl("^Android", active_users$SOURCE , ignore.case=FALSE, fixed=FALSE)])
}

report<-rbind(report,tbadded)
# Redesign tou script kai tou report
# Facebook and cpc check because the they are contained almost everywhere
# Decide where each one should be shown, and distinguish them in the script.

# Finally , make the report lazy so as to calculate only the new rows required for the report.
rm(tbadded)
write.csv(report, file = "wmf.csv",row.names=FALSE)
# Stop timer
proc.time() - ptm