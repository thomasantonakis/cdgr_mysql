# Load package
library(RMySQL)
# Set timer
ptm <- proc.time()
# Establish connection
con <- dbConnect(RMySQL::MySQL(), host = '172.20.0.1', port = 3307, dbname = "beta",
                 user = "tantonakis", password = "2secret4usAll!")
# Send query
rs <- dbSendQuery(con,"

SELECT `mobile`
FROM  `user_master` 
WHERE `status` =  'NEW'
AND  `usertype_id` =100
AND  `is_active` =  'Y'
AND  `is_deleted` =  'N'
AND  `mobile` LIKE '69%'
and  `status` !=  'INVALID'
AND  `sms` = 1
AND  `i_date` >= UNIX_TIMESTAMP(  '2014-01-01' ) 

                  ")
# Fetch query results (n=-1) means all results
d1 <- dbFetch(rs, n=-1) 
# close connection
dbDisconnect(con)
# Stop timer
proc.time() - ptm


# Check who have received SMS previously

# Establish connection
con <- dbConnect(RMySQL::MySQL(), host = '172.20.0.1', port = 3307, dbname = "beta",
                 user = "tantonakis", password = "2secret4usAll!")
# Send query
rs <- dbSendQuery(con,"

SELECT `mobile`
FROM  `sms_phones` 

                  ")
# Fetch query results (n=-1) means all results
d2 <- dbFetch(rs, n=-1) 
# close connection
dbDisconnect(con)
# Stop timer
proc.time() - ptm



# Check who have received SMS previously

# Establish connection
con <- dbConnect(RMySQL::MySQL(), host = '172.20.0.1', port = 3307, dbname = "beta",
                 user = "tantonakis", password = "2secret4usAll!")
# Send query
rs <- dbSendQuery(con,"

SELECT COUNT(*)
FROM  `sms_phones` 
GROUP BY `batch`

                  ")
# Fetch query results (n=-1) means all results
d3 <- dbFetch(rs, n=-1) 
# close connection
dbDisconnect(con)
# Stop timer
proc.time() - ptm


###########################
# exclude from d1 the d2

library(xlsx)


# write.xlsx(x = d1, file = "proposal.xlsx",
#           row.names = FALSE)
# write.xlsx(x = d2, file = "old_sms.xlsx",
#           row.names = FALSE)
# 

write.csv(x = d1, file = "sms_4_2_2015.csv",
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
