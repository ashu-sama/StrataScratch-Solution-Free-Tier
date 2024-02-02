# Import your libraries
import pyspark
from pyspark.sql import Window
from pyspark.sql.functions import col, percent_rank
# Start writing code
window_spec = Window.partitionBy("state").orderBy("fraud_score")
(fraud_score
        .withColumn("pr", percent_rank().over(window_spec))
        .filter(col("pr") >= 0.95)
        .drop("pr")
        .toPandas())