'''
Word Count Analysis (Top 10)

Dataset | Column 
----------------------+
FBPAC   | title

This script was applied to the fbpac dataset in the teamnorth folder. In this script, a group by was used because entities
within title are multiworded and must be treated as one word.

'''

from pyspark.sql.functions import count, desc, col
from pyspark.sql import SparkSession

# Load CSV file into a PySpark DataFrame
spark = SparkSession.builder.master("local[*]").appName("proj2tn").getOrCreate()
df = spark.read.csv("/home/madkins/teamnorth/data/fbpac-ads-en-US.csv",
				inferSchema = True,
				header = True,
				sep = ',',
				#quote = "",
				escape = '"',
				multiLine = True)

df = df.filter(col("title").isNotNull())

# Count instances of each title name using groupBy and count functions
title_counts = df.groupBy("title").agg(count("*").alias("count"))
sorted_title_counts = title_counts.orderBy(desc("count"))

# Show the results
sorted_title_counts.show(10, truncate=False)
# specify the output file path


sorted_title_counts = sorted_title_counts.limit(10)

output_path = "/home/madkins/example/fbpac_title.csv"

# write the output DataFrame to a CSV file
sorted_title_counts.write.format("csv") \
    .option("header", "true") \
    .option("truncate", False) \
    .mode("overwrite") \
    .save(output_path)

