# Import your libraries
import pyspark
from pyspark.sql.functions import col, when
# Start writing code
car_launches_df = (car_launches
                        .select(col("company_name"), 
                                when(col("year") == 2020, 1).otherwise(0).alias("2020"),
                                when(col("year") == 2019, 1).otherwise(0).alias("2019"))
                        .groupBy(col("company_name"))
                        .agg({"2020": 'sum', "2019": 'sum'})
                        .select(col("company_name"), (col("sum(2020)")-col("sum(2019)")).alias("net_products"))
                        .orderBy("company_name"))

# To validate your solution, convert your final pySpark df to a pandas df
car_launches_df.toPandas() 