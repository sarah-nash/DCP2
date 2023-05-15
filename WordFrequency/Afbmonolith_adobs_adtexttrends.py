# The Adtext col trends for fbmonolith (AD-OBSERVER) dataset
# Years broken into quarterly (3 month periods)
# Script returns 15 most common words in each window of time
import pyspark
import nltk
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, regexp_replace, desc, lower, col, count, year, month, concat, lit, dense_rank
from pyspark.ml.feature import StopWordsRemover, Tokenizer
from nltk.corpus import stopwords
from pyspark.sql.window import Window

spark = SparkSession.builder.master("local[*]").appName("project2trends").getOrCreate()

nltk.download('stopwords')
stop_words = stopwords.words('english') + ["us", "help", "people", "need", "get", "span", "time", "like",
               "today", "take", "right", "afzspanspan", "classafxspan", "classcl",
               "new", "classmfrspan", "one", "donate", "join", "back", "please", "every",
               "day", "know", "care", "sign", "classcnspan", "pthe", "gift", "htz",
               "pp", "march", "support", "classimg", "eimg", "ubd", "f", "img", "h", "classcf",
               "classqdmspanspan", "classza", "x", "classmfr", "datahovercardhttpswwwfacebookcom", "november",
               "classprofilelink", "june", "hiddenelem", "classyudiv", "fdiv", "vote", "august", "campaign",
               "classw", "hrefi", "fec", "fight", "would", "im", "name", "dont", "way", "oydivdiv", "much", "big", "election",
               "k", "th", "tell", "keep", "classfwb", "thats", "oh", "fcga", "todayp",
               "comment", "see", "facebook", "year", "give", "page", "de", "donation", "impact", "make",
               "un", "ir", "save", "holiday", "shop", "ad", "affiliated", "provide", "better", "free", "matched", "learn",
               "paid", "food", "match", "en", "february", "paid", "send", "valentines", "first", "form", "interested",
               "want", "home", "years", "pm", "work", "local", "message", "use", "end" ]

#/home/madkins/example/pro_pub_sample_targets.csv

df = spark.read.csv("/home/madkins/example/fbm2.csv",
                    inferSchema=True,
                    header=True,
                    sep=',',
                    escape='"',
                    multiLine=True)

# Clean and tokenize the message column
df_cleaned_adtext = df.select("ad_text", "observed_at").na.drop()
df_cleaned_adtext = df_cleaned_adtext.withColumn("cleaned_text", regexp_replace(df_cleaned_adtext.ad_text, '<[^>]+>', ''))
df_cleaned_adtext = df_cleaned_adtext.withColumn("cleaned_text", regexp_replace(df_cleaned_adtext.ad_text, '[^a-zA-Z\\s]', ''))
df_cleaned_adtext = df_cleaned_adtext.withColumn("cleaned_text", lower(df_cleaned_adtext.cleaned_text))
tokenizer = Tokenizer(inputCol="cleaned_text", outputCol="words")
df_tokenized_adtext = tokenizer.transform(df_cleaned_adtext)
remover = StopWordsRemover(inputCol="words", outputCol="filtered", stopWords=stop_words)
df_cleaned_adtext = remover.transform(df_tokenized_adtext)
df_words_adtext = df_cleaned_adtext.select(explode("filtered").alias("word"), "observed_at").filter(col("word") != '').filter(col("word") != ' ')

# Extract year and month from the observed_at column
df_words_adtext = df_words_adtext.withColumn("year", year(col("observed_at")))
df_words_adtext = df_words_adtext.withColumn("month", month(col("observed_at")))

# Concatenate year and month to form a period identifier
df_words_adtext = df_words_adtext.withColumn("period", concat(col("year"), lit("-"), col("month")))

from pyspark.sql.functions import quarter

# Extract quarter from the "observed_at" column
df_words_adtext = df_words_adtext.withColumn("quarter", quarter(col("observed_at")))

# Define the quarterly groups
quarterly_groups = [
    ("2020-1", "2020-3"),
    ("2020-4", "2020-6"),
    ("2020-7", "2020-9"),
    ("2020-10", "2020-12"),
    ("2021-1", "2021-3"),
    ("2021-4", "2021-6"),
    ("2021-7", "2021-9"),
    ("2021-10", "2021-12")
]

# Filter the DataFrame for each quarterly group and count word frequencies
quarterly_frequency = []
for start_period, end_period in quarterly_groups:
    start_year, start_month = start_period.split("-")
    end_year, end_month = end_period.split("-")

    filtered_df = df_words_adtext.filter(
        (col("year") == int(start_year)) & (col("month") >= int(start_month)) & (col("month") <= int(end_month))
    )
    #filtered_df = df_words_adtext.filter((col("period") >= start_period) & (col("period") <= end_period))
    group_frequency = filtered_df.groupBy("period", "word").count().orderBy(desc("count"))
    quarterly_frequency.append(group_frequency)
    
# Print the output for each quarterly period
for idx, period_frequency in enumerate(quarterly_frequency):
    period_title = quarterly_groups[idx][0] + " to " + quarterly_groups[idx][1]
    print(f"Word frequencies for period: {period_title}")
    if period_frequency.count() > 0:
        period_frequency.orderBy(desc("count")).limit(15).show(truncate=False) #.show(truncate=False) 10
    else:
        print("No data for this period")

# Create an empty DataFrame to hold the consolidated output
consolidated_output = spark.createDataFrame([], schema=quarterly_frequency[0].schema)

# Append the top ten word frequencies for each quarterly period to the consolidated output
for period_frequency in quarterly_frequency:
    top_ten = period_frequency.orderBy(desc("count")).limit(10)
    consolidated_output = consolidated_output.union(top_ten)

# Save the consolidated output to a CSV file
consolidated_output.coalesce(1).write.csv("/home/madkins/example/Afbm_adobs_adtexttrends.csv", header=True)


