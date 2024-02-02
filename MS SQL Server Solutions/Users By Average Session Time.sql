with session as
    (select 
        user_id, 
        timestamp, 
        action, 
        lag(action) 
            over(
                partition by user_id, cast(timestamp as date)
                order by timestamp) prev_action,
        lag(timestamp)
            over(
                partition by user_id, cast(timestamp as date)
                order by timestamp) prev_timestamp
    from 
        facebook_web_log
    where
        action in ('page_load', 'page_exit'))
select
    user_id,
    avg(datediff(s, prev_timestamp, timestamp)*1.0) session_time
from
    session
where
    action = 'page_exit' and prev_action = 'page_load'
group by
    user_id
order by
    session_time desc
