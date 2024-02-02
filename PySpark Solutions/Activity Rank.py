# Import your libraries
from pyspark.sql.functions import col, count, dense_rank
from pyspark.sql.window import Window

# Start writing code
win_spec = Window.orderBy(col('total_emails').desc(), col('user'))
google_gmail_emails = (google_gmail_emails
                            .groupBy(col('from_user').alias('user'))
                            .agg(count('id').alias('total_emails'))
                            .withColumn('activity_rank', dense_rank().over(win_spec))
    )

# To validate your solution, convert your final pySpark df to a pandas df
google_gmail_emails.toPandas()