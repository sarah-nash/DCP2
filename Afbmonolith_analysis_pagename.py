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

df = df.filter(col(" page_name").isNotNull())

# Count instances of each pagename name using groupBy and count functions
pagename_counts = df.groupBy(" page_name").agg(count("*").alias("count"))
sorted_pagename_counts = pagename_counts.orderBy(desc("count"))

# Show the results
sorted_pagename_counts.show(10, truncate=False)
# specify the output file path


sorted_pagename_counts = sorted_pagename_counts.limit(10)

output_path = "/home/madkins/example/Afbmonolith_pagename.csv"

# write the output DataFrame to a CSV file
sorted_pagename_counts.write.format("csv") \
    .option("header", "true") \
    .option("truncate", False) \
    .mode("overwrite") \
    .save(output_path)
