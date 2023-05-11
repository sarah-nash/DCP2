
import pyspark
import nltk
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, regexp_replace, desc, lower, col, count, year, month, concat, lit, dense_rank
from pyspark.ml.feature import StopWordsRemover, Tokenizer
from nltk.corpus import stopwords
from pyspark.sql.window import Window

spark = SparkSession.builder.master("local[*]").appName("project2init").getOrCreate()

nltk.download('stopwords')
stop_words = stopwords.words('english') + ["us", "help", "people", "need", "make", "get", "span", "time", "like",
               "today", "take", "right", "afzspanspan", "classafxspan", "classcl",
               "new", "classmfrspan", "one", "donate", "join", "back", "please", "every",
               "day", "know", "care", "sign", "classcnspan", "pthe", "gift", "htz"]

df = spark.read.csv("/home/madkins/example/pro_pub_sample_targets.csv",
                    inferSchema=True,
                    header=True,
                    sep=',',
                    escape='"',
                    multiLine=True)

# Clean and tokenize the message column
df_cleaned_mssg = df.select("message", "created_at").na.drop()
df_cleaned_mssg = df_cleaned_mssg.withColumn("cleaned_text", regexp_replace(df_cleaned_mssg.message, '<[^>]+>', ''))
df_cleaned_mssg = df_cleaned_mssg.withColumn("cleaned_text", regexp_replace(df_cleaned_mssg.message, '[^a-zA-Z\\s]', ''))
df_cleaned_mssg = df_cleaned_mssg.withColumn("cleaned_text", lower(df_cleaned_mssg.cleaned_text))
tokenizer = Tokenizer(inputCol="cleaned_text", outputCol="words")
df_tokenized_mssg = tokenizer.transform(df_cleaned_mssg)
remover = StopWordsRemover(inputCol="words", outputCol="filtered", stopWords=stop_words)
df_cleaned_mssg = remover.transform(df_tokenized_mssg)
df_words_mssg = df_cleaned_mssg.select(explode("filtered").alias("word"), "created_at").filter(col("word") != '').filter(col("word") != ' ')

# Extract year and month from the created_at column
df_words_mssg = df_words_mssg.withColumn("year", year(col("created_at")))
df_words_mssg = df_words_mssg.withColumn("month", month(col("created_at")))

# Concatenate year and month to form a period identifier
df_words_mssg = df_words_mssg.withColumn("period", concat(col("year"), lit("-"), col("month")))

from pyspark.sql.functions import quarter

# Extract quarter from the "created_at" column
df_words_mssg = df_words_mssg.withColumn("quarter", quarter(col("created_at")))

# Define the quarterly groups
quarterly_groups = [
    ("2018-01", "2018-03"),
    ("2018-04", "2018-06"),
    ("2018-07", "2018-09"),
    ("2018-10", "2018-12"),
    ("2019-01", "2019-03"),
    ("2019-04", "2019-06"),
    ("2019-07", "2019-09"),
    ("2019-10", "2019-12"),
    ("2020-01", "2020-03"),
    ("2020-04", "2020-06"),
    ("2020-07", "2020-09"),
    ("2020-10", "2020-12")
]

# Filter the DataFrame for each quarterly group and count word frequencies
quarterly_frequency = []
for start_period, end_period in quarterly_groups:
    filtered_df = df_words_mssg.filter((col("period") >= start_period) & (col("period") <= end_period))
    group_frequency = filtered_df.groupBy("quarter", "word").count().orderBy(desc("count"))
    quarterly_frequency.append(group_frequency)

for idx, period_frequency in enumerate(quarterly_frequency):
    period_title = quarterly_groups[idx][0] + " to " + quarterly_groups[idx][1]
    period_frequency.show(truncate=False)
    print(f"Word frequencies for period: {period_title}")
    period_frequency.createOrReplaceTempView(f"period_{idx+1}")
    spark.sql(f"SELECT * FROM period_{idx+1} ORDER BY count DESC").limit(10).show(truncate=False)

# Create an empty DataFrame to hold the consolidated output
consolidated_output = spark.createDataFrame([], schema=quarterly_frequency[0].schema)

# Append the top ten word frequencies for each quarterly period to the consolidated output
for period_frequency in quarterly_frequency:
    top_ten = period_frequency.orderBy(desc("count")).limit(10)
    consolidated_output = consolidated_output.union(top_ten)

# Save the consolidated output to a CSV file
consolidated_output.coalesce(1).write.csv("/path/to/output.csv", header=True)