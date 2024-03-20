with dec_unq_usr as (
    select distinct
        format(cast(date as date), 'yyyy-MM') as month,
        account_id,
        user_id
    from sf_events
    where
        format(cast(date as date), 'yyyy-MM') = '2020-12'
),
jan_unq_usr as (
    select distinct
        format(cast(date as date), 'yyyy-MM') as month,
        account_id,
        user_id
    from sf_events
    where
        format(cast(date as date), 'yyyy-MM') = '2021-01'
),
feb_unq_usr as (
    select distinct
        format(cast(date as date), 'yyyy-MM') as month,
        account_id,
        user_id
    from sf_events
    where
        format(cast(date as date), 'yyyy-MM') = '2021-02'
)
select
    dec.account_id,
    count(feb.user_id)/count(jan.user_id) rentention
from
    dec_unq_usr dec
    left join jan_unq_usr jan on dec.user_id = jan.user_id and dec.account_id = jan.account_id
    left join feb_unq_usr feb on feb.user_id = jan.user_id and feb.account_id = jan.account_id
group by
    dec.account_id