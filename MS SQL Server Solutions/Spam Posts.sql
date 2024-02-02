select 
    post_date,
    count(case when post_keywords like '%spam%' then p.post_id end)*100.0/count(*) spam_percent
from 
    facebook_posts p
where exists (  select 
                    viewer_id 
                from 
                    facebook_post_views v
                where p.post_id = v.post_id)
group by post_date