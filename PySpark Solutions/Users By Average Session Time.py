# Import your libraries
from pyspark.sql.functions import col, cast, lag, avg
from pyspark.sql.window import Window
# Start writing code
window_spec = Window.partitionBy('user_id', col('timestamp').cast("date"))\
                    .orderBy('timestamp')
fb_web_log = (facebook_web_log
                    .filter(col('action').isin('page_load','page_exit'))
                    .withColumn('prev_timestamp', lag('timestamp').over(window_spec))
                    .withColumn('prev_action', lag('action').over(window_spec))
                    .filter((col('action') == 'page_exit') & (col('prev_action') == 'page_load'))
                    .select('user_id', (col('timestamp')-col('prev_timestamp')).alias('session_time'))
                    .groupBy('user_id')
                    .agg({'session_time': 'avg'})
                    .orderBy(col('avg(session_time)').desc())
                    
)
# # To validate your solution, convert your final pySpark df to a pandas df
fb_web_log.toPandas()