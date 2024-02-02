with cte as
(
    select 
        id, user_id, created_at, 
        lag(created_at) over(partition by user_id order by created_at) prev_order_date
    from 
        amazon_transactions)
select
    distinct user_id
from
    cte
where
    datediff(day, prev_order_date, created_at) < 7
    