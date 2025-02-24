# Load package
library(RMySQL)
# Set timer
ptm <- proc.time()
# Establish connection
con <- dbConnect(RMySQL::MySQL(), host = '172.20.0.1', port = 3307, dbname = "beta",
                 user = "tantonakis", password = "2secret4usAll!")
# Send query
rs <- dbSendQuery(con,"
                  
                        SELECT `referal_source` as source ,`status` as status , COUNT(*) as count 
                        FROM  `user_master` 
                        WHERE  `referal_source` LIKE  '%larisaout%'
                        AND `i_date` >= UNIX_TIMESTAMP('2015-03-25')
                        AND `i_date` < UNIX_TIMESTAMP('2015-03-30')
                        GROUP BY `referal_source`, `status`
                  
                  
                  ")
# Fetch query results (n=-1) means all results
d1 <- dbFetch(rs, n=-1) 
# close connection
dbDisconnect(con)
# Stop timer
proc.time() - ptm



##### RGA ga:sourceMedium

############################################
startdate='2015-03-25' ##Start Date#########
enddate='2015-03-30' ####End Date###########
############################################
library(RGA)
# Authenticate Google Analytics
client.id = '543269518849-dcdk7eio32jm2i4hf241mpbdepmifj00.apps.googleusercontent.com'
client.secret = '9wSw6gyDVXtcgqEe0XazoBWG'
ga_token<-authorize(client.id, client.secret, cache = getOption("rga.cache"))

analytics<-get_ga(25764841, start.date = startdate, end.date = enddate,
            
            metrics = "
                        ga:sessions,
                        ga:goal1Completions,
                        ga:goal6Completions
                ",
            
            dimensions = "
                        ga:sourceMedium
                ",
            sort = NULL, 
            filters = NULL,
            segment = NULL, 
            sampling.level = NULL,
            start.index = NULL, 
            max.results = NULL, 
            ga_token
)

## cosmoradio einai to source tou campaign
## banner einai to medium tou campaign

analytics[grep("larisaout", analytics$sourceMedium , ignore.case=FALSE, fixed=FALSE),]
View(d1)
proc.time() - ptm
