'''
Word Count Analysis (Top 10)

Dataset | Column 
----------------------+
FBPAC   | advertiser

This script was applied to the fbpac dataset in the teamnorth folder. In this script, a group by was used because entities
within advertiser are multiworded and must be treated as one word.

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

df = df.filter(col("advertiser").isNotNull())

# Count instances of each advertiser name using groupBy and count functions
advertiser_counts = df.groupBy("advertiser").agg(count("*").alias("count"))
sorted_advertiser_counts = advertiser_counts.orderBy(desc("count"))

# Show the results
sorted_advertiser_counts.show(10, truncate=False)
# specify the output file path


sorted_advertiser_counts = sorted_advertiser_counts.limit(10)

output_path = "/home/madkins/example/fbpac_advertiser.csv"

# write the output DataFrame to a CSV file
sorted_advertiser_counts.write.format("csv") \
    .option("header", "true") \
    .option("truncate", False) \
    .mode("overwrite") \
    .save(output_path)

