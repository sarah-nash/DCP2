# This script generates the top 10 most common organizations that funded ads in the Paidforby in fbmonolith (AdObserver) data
from pyspark.sql.functions import count, desc, col
from pyspark.sql import SparkSession
# Load CSV file into a PySpark DataFrame
spark = SparkSession.builder.master("local[*]").appName("proj2tn").getOrCreate()
df = spark.read.csv("/home/madkins/teamnorth/data/fb_monolith.csv",
				inferSchema = True,
				header = True,
				sep = ',',
				#quote = "",
				escape = '"',
				multiLine = True)

df = df.filter(col(" paid_for_by").isNotNull())

# Count instances of each paid4by name using groupBy and count functions
paid4by_counts = df.groupBy(" paid_for_by").agg(count("*").alias("count"))
sorted_paid4by_counts = paid4by_counts.orderBy(desc("count"))

# Show the results
sorted_paid4by_counts.show(10, truncate=False)
# specify the output file path

sorted_paid4by_counts = sorted_paid4by_counts.limit(10)

output_path = "/home/madkins/example/fbm_paid4by.csv"

# write the output DataFrame to a CSV file
sorted_paid4by_counts.write.format("csv") \
    .option("header", "true") \
    .option("truncate", False) \
    .mode("overwrite") \
    .save(output_path)