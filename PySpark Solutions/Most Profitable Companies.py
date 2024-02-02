# Import your libraries
import pyspark
# Start writing code
forbes_global_2010_2014 = (forbes_global_2010_2014
                            .select('company', 'profits')
                            .orderBy(forbes_global_2010_2014.profits, ascending=False)
                            .limit(3)
                        )

# To validate your solution, convert your final pySpark df to a pandas df
forbes_global_2010_2014.toPandas()