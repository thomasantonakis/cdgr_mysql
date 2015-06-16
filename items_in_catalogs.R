# Load package
library(RMySQL)

# Set timer
ptm <- proc.time()

# Establish connection
con <- dbConnect(RMySQL::MySQL(), host = '172.20.0.1', port = 3307, dbname = "beta",
                 user = "tantonakis", password = "2secret4usAll!")
# Send query
rs <- dbSendQuery(con,"
                  
                  
SELECT restaurant_detail.restaurant_name as restname,
        restaurant_master.restaurant_id as restid,
        item_master.catalog_id as catalogid, 
        item_detail.item_name as itemname

FROM item_detail
JOIN item_master
USING (item_id)
JOIN restaurant_master
USING (catalog_id)
JOIN restaurant_detail
USING (restaurant_id)

WHERE

                  item_detail.language_id = 1
                  AND restaurant_detail.language_id = 1
                  AND `restaurant_master`.`start_date` < now()
                  AND `restaurant_id` != 19
                  AND (`restaurant_master`.`signoff_date` IS NULL )
                  AND (`item_detail`.`item_name`  LIKE '%amstel%'
                  OR `item_detail`.`item_name`  LIKE '%heineken%'
                  OR `item_detail`.`item_name`  LIKE '%μύθος%'
                  OR `item_detail`.`item_name`  LIKE '%μυθος%'
                  OR `item_detail`.`item_name`  LIKE '%αλφα%'
                  OR `item_detail`.`item_name`  LIKE '%άλφα%'
                  OR `item_detail`.`item_name`  LIKE '%alfa%'
                  OR `item_detail`.`item_name`  LIKE '%kaiser%'
                  OR `item_detail`.`item_name`  LIKE '%pils%'
                  OR `item_detail`.`item_name`  LIKE '%fischer%'
                  OR `item_detail`.`item_name`  LIKE '%fix%'
                  OR `item_detail`.`item_name`  LIKE '%φιξ%'
                  OR `item_detail`.`item_name`  LIKE '%mythos%'
                  OR `item_detail`.`item_name`  LIKE '%buckler%'
                  OR `item_detail`.`item_name`  LIKE '%μπύρα%'
                  OR `item_detail`.`item_name`  LIKE '%beer%'
                  )
                  
                  
                  
                  
                  ")

# Fetch query results (n=-1) means all results
beers <- dbFetch(rs, n=-1) 
# close connection
dbDisconnect(con)
# Stop timer
proc.time() - ptm

write.csv(x= beers, file="export.csv", row.names=FALSE)
