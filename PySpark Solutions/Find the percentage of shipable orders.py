# Import your libraries
import pyspark
from pyspark.sql.functions import count
# Start writing code
shipable_orders = (orders
                        .join(customers, (orders.cust_id == customers.id) & customers.address.isNotNull(), 'left')
                        .select((count(customers.id)*100/count(orders.id)).alias('shipable_orders'))
                        )
# To validate your solution, convert your final pySpark df to a pandas df
shipable_orders.toPandas()