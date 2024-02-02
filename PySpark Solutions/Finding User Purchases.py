# Import your libraries
from pyspark.sql.functions import col, date_add

# Start writing code
at1 = amazon_transactions.alias('at1')
at2 = amazon_transactions.alias('at2')

active_users = (at1
                    .join(at2, ((col('at1.user_id') == col('at2.user_id')) & (col('at1.id') != col('at2.id'))) & ((col('at2.created_at') < date_add(col('at1.created_at'), 7)) & (col('at1.created_at') <= col('at2.created_at'))))
                    .select(col('at1.user_id'))
                    .distinct()
                    .orderBy(col('at1.user_id')))

# To validate your solution, convert your final pySpark df to a pandas df
active_users.toPandas()