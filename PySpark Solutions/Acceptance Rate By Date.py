# Import your libraries
import pyspark
from pyspark.sql.functions import col, count
# Start writing code
fb_sent = (fb_friend_requests
                .filter(col("action") == "sent")
                .select(*(col(x).alias("s_" + x) for x in fb_friend_requests.columns)))
# fb_sent.toPandas()

fb_accepted = (fb_friend_requests
                .filter(col("action") == "accepted")
                .select(*(col(x).alias("a_" + x) for x in fb_friend_requests.columns)))
# fb_accepted.toPandas()

sent_accepted = (fb_sent
                    .join(fb_accepted, 
                          (col("s_user_id_sender")==col("a_user_id_sender")) & 
                          (col("s_user_id_receiver")==col("a_user_id_receiver")),
                          "left")
                    .groupBy(col("s_date"))
                    .agg((count(col("a_action"))/count(col("s_action"))).alias("percentage_acceptance"))
                    )
sent_accepted.toPandas()