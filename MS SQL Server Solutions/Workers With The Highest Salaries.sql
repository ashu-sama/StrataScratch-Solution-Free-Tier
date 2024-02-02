select worker_title
from (  select
            t.worker_title, rank() over(order by salary desc) rank
        from 
            worker w
            join title t on w.worker_id = t.worker_ref_id) tbl
where rank = 1