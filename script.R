# library(DBI)
# 
# library("RMySQL", lib.loc="~/R/win-library/3.1")
# 
# con <- dbConnect(RMySQL::MySQL(), host = 'db.clickdelivery.gr', port = 3307, dbname = "beta",
#                  user = "tantonakis", password = "2secret4usAll!")
# m<-MySQL(max.con = 16, fetch.default.rec = 15000)
# con2 <- dbConnect(m, host = 'db.clickdelivery.gr',port = 3307, dbname = "beta" )
# summary(con)
# dbDisconnect(con)



library(RODBC)
library("RMySQL", lib.loc="~/R/win-library/3.1")
# db <- odbcConnect("cdgr_mysql" )
# db <- odbcConnect("cdgr_mysql" ,uid="tantonakis", pwd="2secret4usAll")
# db <- dbConnect(RMySQL::MySQL(), dbname = "beta")

con <- dbConnect(RMySQL::MySQL(), host = 'db.clickdelivery.gr', port = 3307, dbname = "beta",
                                  user = "tantonakis", password = "2secret4usAll!")
rs <- dbSendQuery(con,"SELECT  `first_name` ,  `last_name` 
                  FROM  `user_master` 
                  WHERE  `user_id` =4
                  LIMIT 0 , 30")
d1 <- dbFetch(rs, n = 10)               
dbDisconnect(con)


# if (mysqlHasDefault()) {
#         # connect to a database and load some data
#         con <- dbConnect(RMySQL::MySQL(), dbname = "test")
#         dbWriteTable(con, "USArrests", datasets::USArrests, overwrite = TRUE)
#         # query
#         rs <- dbSendQuery(con, "SELECT * FROM USArrests")
#         d1 <- dbFetch(rs, n = 10) # extract data in chunks of 10 rows
#         dbHasCompleted(rs)
#         d2 <- dbFetch(rs, n = -1) # extract all remaining data
#         dbHasCompleted(rs)
#         dbClearResult(rs)
#         dbListTables(con)
#         # clean up
#         dbRemoveTable(con, "USArrests")
#         dbDisconnect(con)
# }