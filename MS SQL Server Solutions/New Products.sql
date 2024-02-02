select 
    company_name, 
    (sum(case when year = 2020 then 1 else 0 end) - sum(case when year = 2019 then 1 else 0 end)) net_products
from car_launches
group by company_name
order by company_name