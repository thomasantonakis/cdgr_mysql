# Load package
library(RMySQL)
library(xlsx)

# Set timer
ptm <- proc.time()

### COmpany report
# Establish connection
con <- dbConnect(RMySQL::MySQL(), host = '172.20.0.1', port = 3307, dbname = "beta",
                 user = "tantonakis", password = "2secret4usAll!")
# Send query
rs <- dbSendQuery(con,"

SELECT
        LEFT(promo_id , 3) AS COUPON_SERIES,
        COUNT(*) AS ISSUED_COUPONS,
        SUM(IF((userpromo.promo_id RLIKE CONCAT('^', LEFT(userpromo.promo_id, 3)) AND user.verification_date < userpromo.valid_after AND user.status = 'VERIFIED'), 1, 0)) AS OLD,
        SUM(IF((userpromo.promo_id RLIKE CONCAT('^', LEFT(userpromo.promo_id, 3)) AND user.verification_date >= userpromo.valid_after AND user.status = 'VERIFIED'), 1, 0)) AS NEW,
        SUM(IF((userpromo.promo_id RLIKE CONCAT('^', LEFT(userpromo.promo_id, 3)) AND user.status IN ('NEW','MANUAL')), 1, 0)) AS UNVERIFIED,
        SUM(IF((userpromo.promo_id RLIKE CONCAT('^', LEFT(userpromo.promo_id, 3)) AND user.status IN ('VERIFIED','NEW','MANUAL')), 1, 0)) AS VERIFIED
        

        FROM user_promos AS userpromo
        LEFT JOIN user_master AS `user`
        ON (`userpromo`.`user_id` != 0 AND `user`.`user_id` = `userpromo`.`user_id`)

        GROUP BY COUPON_SERIES


 ")

# Fetch query results (n=-1) means all results
coupons <- dbFetch(rs, n=-1) 
# close connection
dbDisconnect(con)
# Stop timer
proc.time() - ptm

setwd("C:/Users/tantonakis/Google Drive/MARKETING MIX/EVENTs/Stts")

write.xlsx(x= coupons, file= "working.xlsx", sheetName="Worksheet", row.names=FALSE)

setwd("C:/Users/tantonakis/Google Drive/Scripts/AnalyticsProj/cdgr_mysql")