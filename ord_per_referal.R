# Load package
library(RMySQL)
library(plyr)
library(xlsx)

# Set timer
ptm <- proc.time()

# Establish connection
con <- dbConnect(RMySQL::MySQL(), host = '172.20.0.1', port = 3307, dbname = "beta",
                 user = "tantonakis", password = "2secret4usAll!")
# Send query
rs <- dbSendQuery(con,"
                  

SELECT 
                                YEAR(FROM_UNIXTIME(`order_master`.`i_date`)) AS YEAR, 
                                MONTH(FROM_UNIXTIME(`order_master`.`i_date`)) AS MONTH,
        			
                                `order_master`.`order_referal` as referral,
				count(`order_master`.`order_id`) AS `ORDERS`,
				ROUND(SUM(`order_master`.`order_amt`),2) AS `ORDERSUM`,
				ROUND(SUM(`order_master`.`order_commission`),2) AS `COMMISION`
				
				FROM `order_master`
				
				
                                where ((`order_master`.`is_deleted` = 'N') and ((`order_master`.`status` = 'VERIFIED') or (`order_master`.`status` = 'REJECTED')))
				AND `order_master`.`i_date` >= UNIX_TIMESTAMP('2015-07-01')
				AND `order_master`.`i_date` < UNIX_TIMESTAMP('2015-08-01')

				GROUP BY  referral,month, year
				
                  
                  "


)
# Fetch query results (n=-1) means all results
data <- dbFetch(rs, n=-1) 
# close connection
dbDisconnect(con)
# Stop timer
proc.time() - ptm

# Write CSV in R
write.csv2(data, file = "export.csv",row.names=FALSE)


# write.xlsx(x=data, file='commissions.xlsx')
