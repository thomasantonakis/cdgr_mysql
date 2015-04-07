# Load package
library(RMySQL)
library(dplyr)
# Set timer
ptm <- proc.time()
# Establish connection
con <- dbConnect(RMySQL::MySQL(), host = '172.20.0.1', port = 3307, dbname = "beta",
                 user = "tantonakis", password = "2secret4usAll!")
# Send query
rs <- dbSendQuery(con,"

SELECT `user_id`,`mobile`
FROM  `user_master` 
WHERE `status` =  'NEW'
AND  `usertype_id` =100
AND  `is_active` =  'Y'
AND  `is_deleted` =  'N'
AND  `mobile` LIKE '69%'
and  `status` !=  'INVALID'
AND  `sms` = 1
AND  `i_date` >= UNIX_TIMESTAMP(  '2015-02-9' ) 
AND  `i_date` <= UNIX_TIMESTAMP(  '2015-03-11' )

                  ")
# Fetch query results (n=-1) means all results
d1 <- dbFetch(rs, n=-1) 
# close connection
dbDisconnect(con)
# Stop timer
proc.time() - ptm

##################################################
# Sort
# Filter unreasonable
# Remove duplicates
# batch +1
##################################################



load("C:/Users/tantonakis/Google Drive/Scripts/AnalyticsProj/cdgr_mysql/Orders_form_2014.RData")
d2<-ord_user[ord_user$days_idle >=180,]
users<-unique(orders[,c(3,6)])
mobiles<-merge(d2, users, by = 'user_id')
mobiles<-mobiles[,c(1,6)]
mobiles<-mobiles[mobiles$mobile !="",]
mobiles$user_id<-NULL

d2<-rbind(d1,mobiles)
d2<-unique(d2)
d2$mobile<-as.numeric(d2$mobile)
d2<-d2[!is.na(d2$mobile),]
d2<-arrange(d2,mobile)
d2<-d2[d2$mobile>=6900000000,]
d2<-d2[d2$mobile<=6999999999,]

library(xlsx)


# write.xlsx(x = d1, file = "proposal.xlsx",
#           row.names = FALSE)
# write.xlsx(x = d2, file = "old_sms.xlsx",
#           row.names = FALSE)
# 

write.csv(x = d2, file = "sms_11_3_2015.csv",
            row.names = FALSE)




# csv me kinita mono
# 
# batch 1: 4/9/2014 
# batch 2: 7/9/2014
# batch 3: 21/11/2014 (athens osoi eixan promo, salonica osoi eixan promo , aleksandroupoli, kozani, agrinio, irakleio, kavala, kalamata, patra, ioannina, serres, 
#                      volos + larisa (eggrafes apo 1/9/ 2014 k meta) )
# batch 4: 25/11/2014 (larisa ektos batch 3)
# batch 5: 27/11/2014 (diam7ath, diam2ath, zografou, kaisariani, peiraias )
# batch 6: 27/11/2014 (rest diam athens, peristeri, aigaleo, ilion, nikaia, petroupoli, moshato, koridallos, chaidari, ag.anargyroi, ag.varvara)
