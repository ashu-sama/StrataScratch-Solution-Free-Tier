select 
    count(case when address is not null then 1 end)*100.0/count(*) percent_shipable
from 
    orders o
    join customers c
        on o.cust_id = c.id