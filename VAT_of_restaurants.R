# Load package
library(RMySQL)

# Set timer
ptm <- proc.time()

### COmpany report

# Establish connection
con <- dbConnect(RMySQL::MySQL(), host = '172.20.0.1', port = 3307, dbname = "beta",
                 user = "tantonakis", password = "2secret4usAll!")
# Send query
rs <- dbSendQuery(con,"

SELECT `restaurant_master`.`restaurant_shortname` as name,
`company_master` .`vat_id`  as VAT

FROM `restaurant_master` 
LEFT JOIN `company_master`
ON (  `restaurant_master` .`company_id` =`company_master` .`company_id` )



WHERE  `accepts_prepaid_transactions` =1

           "
                  
                  
)
# Fetch query results (n=-1) means all results
mouratos <- dbFetch(rs, n=-1) 
# close connection
dbDisconnect(con)
# Stop timer
proc.time() - ptm

# Write CSV in R
write.csv(mouratos, file = "mouratos.csv",row.names=FALSE)