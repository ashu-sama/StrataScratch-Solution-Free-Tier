select 
    language, 
    count(distinct
        case 
            when device In ('ipad air', 'iphone 5s', 'macbook pro')
            then e.user_id
        end) n_apple_users,
    count(distinct e.user_id) n_total_users
from 
    playbook_events e
    join playbook_users u on e.user_id = u.user_id
group by language
order by n_total_users desc
