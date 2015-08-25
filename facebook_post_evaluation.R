#########################################
# Verified Users From mySQL
#########################################
# Set timer
ptm <- proc.time()
# Load package
library(RMySQL)

# Establish connection
con <- dbConnect(RMySQL::MySQL(), host = '172.20.0.1', port = 3307, dbname = "beta",
                 user = "tantonakis", password = "2secret4usAll!")
# Send query
rs <- dbSendQuery(con,"
                  
                  SELECT COUNT(*) AS USERS,
                  `user_master`.`referal_source` AS SOURCE,
                  date(FROM_UNIXTIME(`user_master`.`verification_date`)) as date
                  
                  
                  FROM `user_master`
                  
                  
                  WHERE `user_master`.`verification_date` >= UNIX_TIMESTAMP('2015-08-01')
                  AND `user_master`.`verification_date` < UNIX_TIMESTAMP('2015-09-01')
                  AND `user_master`.`status` = 'VERIFIED'
                  AND `user_master`.`is_deleted` = 'N'
                  AND `user_master`.`referal_source` LIKE '%facebook|cpc%'
                  

                  GROUP BY `user_master`.`referal_source` , date
                  
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
                  
                  SELECT COUNT(*) AS USERS, 
                  `user_master`.`referal_source` AS SOURCE,
                  date(FROM_UNIXTIME(`user_master`.`i_date`)) as date
                  FROM `user_master`
                  WHERE `user_master`.`i_date` >= UNIX_TIMESTAMP('2015-08-01')
                  AND `user_master`.`i_date` < UNIX_TIMESTAMP('2015-09-01')
                  AND `user_master`.`is_deleted` = 'N'
                  AND `user_master`.`referal_source` LIKE '%facebook|cpc%'
                  GROUP BY `user_master`.`referal_source`, date
                  
                  ")
# Fetch query results (n=-1) means all results
registered_src <- dbFetch(rs, n=-1) 

# close connection
dbDisconnect(con)
# Stop timer
proc.time() - ptm

registered_src$device<-sapply(strsplit(x = registered_src$SOURCE, split = "\\|"),"[[", 1)
registered_src$banner<-sapply(strsplit(x = registered_src$SOURCE, split = "\\|"),"[[", 5)
verified_src$device<-sapply(strsplit(x = verified_src$SOURCE, split = "\\|"),"[[", 1)
verified_src$banner<-sapply(strsplit(x = verified_src$SOURCE, split = "\\|"),"[[", 5)
registered_src$SOURCE<-NULL
verified_src$SOURCE<-NULL
registered_src$metric<-"r"
verified_src$metric<-"v"

registered_src$banner<-gsub(pattern = 'dominos_triri_1\\+1_orange', replacement = 'dominos_orange_triti_1\\+1', x = registered_src$banner)
registered_src$banner<-gsub(pattern = 'dominos_triri_1\\+1_red', replacement = 'dominos_red_triti_1\\+1', x = registered_src$banner)
registered_src$banner<-gsub(pattern = 'dominos_triri_1\\+1_yellow', replacement = 'dominos_yellow_triti_1\\+1', x = registered_src$banner)
registered_src$banner<-gsub(pattern = 'fan_2pizzas_10euro_orange', replacement = 'fan_2p_10e_orange', x = registered_src$banner)
registered_src$banner<-gsub(pattern = 'fan_2pizzas_10euro_red', replacement = 'fan_2p_10e_red', x = registered_src$banner)
registered_src$banner<-gsub(pattern = 'fan_2pizzas_10euro_yellow', replacement = 'fan_2p_10e_yellow', x = registered_src$banner)
registered_src$banner<-gsub(pattern = 'goodys_extreme_990_orange', replacement = 'goodys_extreme_9.90_orange', x = registered_src$banner)
registered_src$banner<-gsub(pattern = 'goodys_extreme_990_red', replacement = 'goodys_extreme_9.90_red', x = registered_src$banner)
registered_src$banner<-gsub(pattern = 'goodys_extreme_990_yellow', replacement = 'goodys_extreme_9.90_yellow', x = registered_src$banner)
registered_src$banner<-gsub(pattern = 'goodys_menu_595_orange', replacement = 'goodys_menu_5.95_orange', x = registered_src$banner)
registered_src$banner<-gsub(pattern = 'goodys_menu_595_red', replacement = 'goodys_menu_5.95_red', x = registered_src$banner)
registered_src$banner<-gsub(pattern = 'goodys_menu_595_yellow', replacement = 'goodys_menu_5.95_yellow', x = registered_src$banner)
registered_src$banner<-gsub(pattern = 'fan_2pizzas_12euro_orange', replacement = 'fan_2p_12e_orange', x = registered_src$banner)
registered_src$banner<-gsub(pattern = 'fan_2pizzas_12euro_red', replacement = 'fan_2p_12e_red', x = registered_src$banner)
registered_src$banner<-gsub(pattern = 'fan_2pizzas_12euro_yellow', replacement = 'fan_2p_12e_yellow', x = registered_src$banner)
registered_src$banner<-gsub(pattern = 'goodys_extreme_1080_orange', replacement = 'goodys_extreme_10.80_orange', x = registered_src$banner)
registered_src$banner<-gsub(pattern = 'goodys_extreme_1080_red', replacement = 'goodys_extreme_10.80_red', x = registered_src$banner)
registered_src$banner<-gsub(pattern = 'goodys_extreme_1080_yellow', replacement = 'goodys_extreme_10.80_yellow', x = registered_src$banner)
registered_src$banner<-gsub(pattern = 'goodys_menu_650_orange', replacement = 'goodys_menu_6.50_orange', x = registered_src$banner)
registered_src$banner<-gsub(pattern = 'goodys_menu_650_red', replacement = 'goodys_menu_6.50_red', x = registered_src$banner)
registered_src$banner<-gsub(pattern = 'goodys_menu_650_yellow', replacement = 'goodys_menu_6.50_yellow', x = registered_src$banner)
registered_src$device<-gsub(pattern = 'Android', replacement = 'Android Generic', x = registered_src$device)
registered_src$device<-gsub(pattern = 'IOS', replacement = 'IOS Generic', x = registered_src$device)

verified_src$banner<-gsub(pattern = 'dominos_triri_1\\+1_orange', replacement = 'dominos_orange_triti_1\\+1', x = verified_src$banner)
verified_src$banner<-gsub(pattern = 'dominos_triri_1\\+1_red', replacement = 'dominos_red_triti_1\\+1', x = verified_src$banner)
verified_src$banner<-gsub(pattern = 'dominos_triri_1\\+1_yellow', replacement = 'dominos_yellow_triti_1\\+1', x = verified_src$banner)
verified_src$banner<-gsub(pattern = 'fan_2pizzas_10euro_orange', replacement = 'fan_2p_10e_orange', x = verified_src$banner)
verified_src$banner<-gsub(pattern = 'fan_2pizzas_10euro_red', replacement = 'fan_2p_10e_red', x = verified_src$banner)
verified_src$banner<-gsub(pattern = 'fan_2pizzas_10euro_yellow', replacement = 'fan_2p_10e_yellow', x = verified_src$banner)
verified_src$banner<-gsub(pattern = 'goodys_extreme_990_orange', replacement = 'goodys_extreme_9.90_orange', x = verified_src$banner)
verified_src$banner<-gsub(pattern = 'goodys_extreme_990_red', replacement = 'goodys_extreme_9.90_red', x = verified_src$banner)
verified_src$banner<-gsub(pattern = 'goodys_extreme_990_yellow', replacement = 'goodys_extreme_9.90_yellow', x = verified_src$banner)
verified_src$banner<-gsub(pattern = 'goodys_menu_595_orange', replacement = 'goodys_menu_5.95_orange', x = verified_src$banner)
verified_src$banner<-gsub(pattern = 'goodys_menu_595_red', replacement = 'goodys_menu_5.95_red', x = verified_src$banner)
verified_src$banner<-gsub(pattern = 'goodys_menu_595_yellow', replacement = 'goodys_menu_5.95_yellow', x = verified_src$banner)
verified_src$banner<-gsub(pattern = 'fan_2pizzas_12euro_orange', replacement = 'fan_2p_12e_orange', x = verified_src$banner)
verified_src$banner<-gsub(pattern = 'fan_2pizzas_12euro_red', replacement = 'fan_2p_12e_red', x = verified_src$banner)
verified_src$banner<-gsub(pattern = 'fan_2pizzas_12euro_yellow', replacement = 'fan_2p_12e_yellow', x = verified_src$banner)
verified_src$banner<-gsub(pattern = 'goodys_extreme_1080_orange', replacement = 'goodys_extreme_10.80_orange', x = verified_src$banner)
verified_src$banner<-gsub(pattern = 'goodys_extreme_1080_red', replacement = 'goodys_extreme_10.80_red', x = verified_src$banner)
verified_src$banner<-gsub(pattern = 'goodys_extreme_1080_yellow', replacement = 'goodys_extreme_10.80_yellow', x = verified_src$banner)
verified_src$banner<-gsub(pattern = 'goodys_menu_650_orange', replacement = 'goodys_menu_6.50_orange', x = verified_src$banner)
verified_src$banner<-gsub(pattern = 'goodys_menu_650_red', replacement = 'goodys_menu_6.50_red', x = verified_src$banner)
verified_src$banner<-gsub(pattern = 'goodys_menu_650_yellow', replacement = 'goodys_menu_6.50_yellow', x = verified_src$banner)
verified_src$device<-gsub(pattern = 'Android', replacement = 'Android Generic', x = verified_src$device)
verified_src$device<-gsub(pattern = 'IOS', replacement = 'IOS Generic', x = verified_src$device)

write.csv2(x = rbind(registered_src,verified_src), file = "export.csv", row.names = FALSE)

# Time!
proc.time() - ptm