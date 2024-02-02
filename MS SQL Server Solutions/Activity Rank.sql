select
    from_user, count(id) total_emails, 
    row_number() over(order by count(id) desc, from_user)  row_number
from 
    google_gmail_emails
group by
    from_user