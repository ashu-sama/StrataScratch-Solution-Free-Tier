with message_sum as
(
    select id_guest, sum(n_messages) sum_n_messages
    from airbnb_contacts
    group by id_guest)
select 
    dense_rank() over(order by sum_n_messages desc) ranking,
    id_guest,
    sum_n_messages
from
    message_sum