select nationality, count(distinct unit_id) num_of_apartments
from 
    airbnb_hosts h
    left join airbnb_units u
        on h.host_id=u.host_id
where 
    unit_type = 'Apartment' and age < 30
group by nationality
order by num_of_apartments desc