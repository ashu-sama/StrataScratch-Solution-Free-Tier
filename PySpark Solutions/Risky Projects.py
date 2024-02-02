# Import your libraries
import pyspark
from pyspark.sql.functions import datediff, col, sum, ceil
# Start writing code
projects = (linkedin_projects
                        .withColumn("duration", datediff(col("end_date"), col("start_date"))/365)
                        .select(col("id"), col("title"), col("budget"), col("duration")))

employee = (linkedin_employees
                        .select(col("id").alias("emp_id"), col("salary")))

relation = (employee
                .join(linkedin_emp_projects, "emp_id", "right")
                .groupBy("project_id")
                .agg(sum("salary").alias("agg_sal"))
                .select(col("project_id").alias("id"), "agg_sal"))
risky_projects = (relation
                    .join(projects, "id", "inner")
                    .select("title", "budget", ceil((col("agg_sal")*col("duration"))).alias("prorated_expense"))
                    .filter(col("budget") < col("prorated_expense"))
                    .orderBy("title"))
# To validate your solution, convert your final pySpark df to a pandas df
risky_projects.toPandas()