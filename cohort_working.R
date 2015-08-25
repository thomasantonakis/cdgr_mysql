SHOW CREATE VIEW





CREATE ALGORITHM=UNDEFINED DEFINER=`root`@`localhost` SQL SECURITY DEFINER VIEW `VIEW_CUSTOMERS_ORDERS_SUMS_SINCE_REGISTRATION_PER_MONTH` AS 

select  `VIEW_VER_USER_ORDERS_AND_SUMS_SINCE_REGISTRATION_PER_MONTH`.`YEAR` AS `YEAR`,
        `VIEW_VER_USER_ORDERS_AND_SUMS_SINCE_REGISTRATION_PER_MONTH`.`MONTH` AS `MONTH`,
        `VIEW_VER_USER_ORDERS_AND_SUMS_SINCE_REGISTRATION_PER_MONTH`.`MONTHS` AS `MONTHS`,
        `VIEW_NEW_VER_CUSTOMERS_PER_MONTH`.`USERS` AS `USERS`,
        `VIEW_VER_USER_ORDERS_AND_SUMS_SINCE_REGISTRATION_PER_MONTH`.`ORDERS` AS `ORDERS`,
        `VIEW_VER_USER_ORDERS_AND_SUMS_SINCE_REGISTRATION_PER_MONTH`.`ORDERSUM` AS `ORDERSUM` 

from 

(`VIEW_VER_USER_ORDERS_AND_SUMS_SINCE_REGISTRATION_PER_MONTH` 

join `VIEW_NEW_VER_CUSTOMERS_PER_MONTH` 

on (((`VIEW_NEW_VER_CUSTOMERS_PER_MONTH`.`YEAR` = `VIEW_VER_USER_ORDERS_AND_SUMS_SINCE_REGISTRATION_PER_MONTH`.`YEAR`) 
    and (`VIEW_NEW_VER_CUSTOMERS_PER_MONTH`.`MONTH` = `VIEW_VER_USER_ORDERS_AND_SUMS_SINCE_REGISTRATION_PER_MONTH`.`MONTH`)))


) 


order by `VIEW_VER_USER_ORDERS_AND_SUMS_SINCE_REGISTRATION_PER_MONTH`.`YEAR`,
        `VIEW_VER_USER_ORDERS_AND_SUMS_SINCE_REGISTRATION_PER_MONTH`.`MONTH`,
        `VIEW_VER_USER_ORDERS_AND_SUMS_SINCE_REGISTRATION_PER_MONTH`.`MONTHS`






CREATE ALGORITHM=UNDEFINED DEFINER=`root`@`localhost` SQL SECURITY DEFINER VIEW `VIEW_VER_USER_ORDERS_AND_SUMS_SINCE_REGISTRATION_PER_MONTH` AS 

select year(from_unixtime(`user_master`.`verification_date`)) AS `YEAR`,
        month(from_unixtime(`user_master`.`verification_date`)) AS `MONTH`,
        floor((((abs((`order_master`.`i_date` - `user_master`.`verification_date`)) / 3600) / 24) / 30.416)) AS `MONTHS`,
        count(`order_master`.`order_id`) AS `ORDERS`,
        sum(`order_master`.`order_amt`) AS `ORDERSUM`,
        sum(`order_master`.`order_commission`) AS `COMMISSIONSUM` 

from (`user_master` 
      
      join `order_master` 
      
      on((`user_master`.`user_id` = `order_master`.`user_id`))) 

where ((`user_master`.`status` = 'VERIFIED') 
       and (`order_master`.`is_deleted` = 'N') 
       and ((`order_master`.`status` = 'VERIFIED') 
            or (`order_master`.`status` = 'REJECTED'))) 

group by year(from_unixtime(`user_master`.`verification_date`)),
        month(from_unixtime(`user_master`.`verification_date`)),
        floor((((abs((`order_master`.`i_date` - `user_master`.`verification_date`)) / 3600) / 24) / 30.416))





CREATE ALGORITHM=UNDEFINED DEFINER=`root`@`localhost` SQL SECURITY DEFINER VIEW `VIEW_NEW_VER_CUSTOMERS_PER_MONTH` AS 

select year(from_unixtime(`user_master`.`verification_date`)) AS `YEAR`,
        month(from_unixtime(`user_master`.`verification_date`)) AS `MONTH`,
        count(`user_master`.`user_id`) AS `USERS` 

from `user_master` 

where ((`user_master`.`is_deleted` = 'N') 
       and (`user_master`.`status` = 'VERIFIED')) 

group by year(from_unixtime(`user_master`.`verification_date`)),
        month(from_unixtime(`user_master`.`verification_date`))








CREATE ALGORITHM=UNDEFINED DEFINER=`root`@`localhost` SQL SECURITY DEFINER VIEW `VIEW_CUSTOMERS_ORDERS_SUMS_SINCE_REGISTRATION_PER_WEEK` AS 

select 
`VIEW_VER_USER_ORDERS_AND_SUMS_SINCE_REGISTRATION_PER_WEEK`.`YEAR` AS `YEAR`,
`VIEW_VER_USER_ORDERS_AND_SUMS_SINCE_REGISTRATION_PER_WEEK`.`WEEK` AS `WEEK`,
`VIEW_VER_USER_ORDERS_AND_SUMS_SINCE_REGISTRATION_PER_WEEK`.`WEEKS` AS `WEEKS`,
`VIEW_NEW_VER_CUSTOMERS_PER_WEEK`.`USERS` AS `USERS`,
`VIEW_VER_USER_ORDERS_AND_SUMS_SINCE_REGISTRATION_PER_WEEK`.`ORDERS` AS `ORDERS`,
`VIEW_VER_USER_ORDERS_AND_SUMS_SINCE_REGISTRATION_PER_WEEK`.`ORDERSUM` AS `ORDERSUM` 

from (`VIEW_VER_USER_ORDERS_AND_SUMS_SINCE_REGISTRATION_PER_WEEK` 
      
      join `VIEW_NEW_VER_CUSTOMERS_PER_WEEK` 
      
      on(((`VIEW_NEW_VER_CUSTOMERS_PER_WEEK`.`YEAR` = `VIEW_VER_USER_ORDERS_AND_SUMS_SINCE_REGISTRATION_PER_WEEK`.`YEAR`) 
      and (`VIEW_NEW_VER_CUSTOMERS_PER_WEEK`.`WEEK` = `VIEW_VER_USER_ORDERS_AND_SUMS_SINCE_REGISTRATION_PER_WEEK`.`WEEK`)))) 

order by 
`VIEW_VER_USER_ORDERS_AND_SUMS_SINCE_REGISTRATION_PER_WEEK`.`YEAR`,
`VIEW_VER_USER_ORDERS_AND_SUMS_SINCE_REGISTRATION_PER_WEEK`.`WEEK`,
`VIEW_VER_USER_ORDERS_AND_SUMS_SINCE_REGISTRATION_PER_WEEK`.`WEEKS`


CREATE ALGORITHM=UNDEFINED DEFINER=`root`@`localhost` SQL SECURITY DEFINER VIEW `VIEW_VER_USER_ORDERS_AND_SUMS_SINCE_REGISTRATION_PER_WEEK` AS 

select
year(from_unixtime(`user_master`.`verification_date`)) AS `YEAR`,
week(from_unixtime(`user_master`.`verification_date`),0) AS `WEEK`,
floor((((abs((`order_master`.`i_date` - `user_master`.`verification_date`)) / 3600) / 24) / 7)) AS `WEEKS`,
count(`order_master`.`order_id`) AS `ORDERS`,
sum(`order_master`.`order_amt`) AS `ORDERSUM`,
sum(`order_master`.`order_commission`) AS `COMMISSIONSUM`

from (`user_master`
      
      join `order_master` 
      on((`user_master`.`user_id` = `order_master`.`user_id`))) 

where ((`user_master`.`status` = 'VERIFIED') 
       and (`order_master`.`is_deleted` = 'N') 
       and ((`order_master`.`status` = 'VERIFIED') 
            or (`order_master`.`status` = 'REJECTED'))) 

group by 
year(from_unixtime(`user_master`.`verification_date`)),
week(from_unixtime(`user_master`.`verification_date`),0),
floor((((abs((`order_master`.`i_date` - `user_master`.`verification_date`)) / 3600) / 24) / 7))


CREATE ALGORITHM=UNDEFINED DEFINER=`root`@`localhost` SQL SECURITY DEFINER VIEW `VIEW_NEW_VER_CUSTOMERS_PER_WEEK` AS 
select 
year(from_unixtime(`user_master`.`verification_date`)) AS `YEAR`,
week(from_unixtime(`user_master`.`verification_date`),0) AS `WEEK`,
count(`user_master`.`user_id`) AS `USERS` 

from `user_master` 

where ((`user_master`.`is_deleted` = 'N') and (`user_master`.`status` = 'VERIFIED')) 

group by year(from_unixtime(`user_master`.`verification_date`)),
week(from_unixtime(`user_master`.`verification_date`),0)

