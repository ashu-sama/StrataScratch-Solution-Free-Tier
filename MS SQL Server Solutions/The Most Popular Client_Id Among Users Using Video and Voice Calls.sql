 with cte as (
    select
        e1.user_id, e1.client_id
    from 
        fact_events e1
        left join fact_events e2 
            on e1.id = e2.id
            and e2.event_type in ('video call received', 'video call sent', 'voice call received', 'voice call sent')
    group by
        e1.user_id, e1.client_id
    having
        (count(e2.event_type)*100.0/count(e1.event_type)) >= 50
),
popular_client as (
    select
        client_id, count(client_id) cnt
    from
        cte
    group by
        client_id
)
select
    top 1
    client_id
from
    popular_client
order by cnt desc