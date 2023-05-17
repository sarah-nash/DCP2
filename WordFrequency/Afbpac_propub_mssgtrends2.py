# The Message col trends for fbpac (PRO-PUBLICA) dataset
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
               "k", "th", "tell", "keep", "classfwb", "thats", "oh", "fcga", "todayp", "send", "going", 
               "first", "win", "state", "stop", "yq", "ym", "make", "yo", "hiu", "classv", "uicontextuallayerparent", "ground", "free",
               "want", "classn", "even", "end", "good", "pv", "stage", "add", "classcn", "div", "give", "year", "years", 
               "see", "many", "give", "br", "public", "stand", "action", "voting", "still", "p", "find",
               "country", "families", "running", "weve", "really", "amp", "deadline", "debate", "working",
               "classmbs", "trumps", "political", "better", "go", "indivisible", "million", "nowp", "fighting", "district",
               "let", "around", "well", "donation", "debates", "classtextexposedhidespanspan", "could", "message",
               "midnight", "share", "providing", "protect", "pdb", "come", "house", "pbx", "click", "hope",
               "class", "special", "race"]

#/home/madkins/example/pro_pub_sample_targets.csv

df = spark.read.csv("/home/madkins/example/pro_pub_targets_entire.csv",
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
    ("2018-1", "2018-3"),
    ("2018-4", "2018-6"),
    ("2018-7", "2018-9"),
    ("2018-10", "2018-12"),
    ("2019-1", "2019-3"),
    ("2019-4", "2019-6"),
    ("2019-7", "2019-9"),
    ("2019-10", "2019-12")
]

# Filter the DataFrame for each quarterly group and count word frequencies
quarterly_frequency = []
for start_period, end_period in quarterly_groups:
    start_year, start_month = start_period.split("-")
    end_year, end_month = end_period.split("-")

    filtered_df = df_words_mssg.filter(
        (col("year") == int(start_year)) & (col("month") >= int(start_month)) & (col("month") <= int(end_month))
    )
    #filtered_df = df_words_mssg.filter((col("period") >= start_period) & (col("period") <= end_period))
    group_frequency = filtered_df.groupBy("word").count().orderBy(desc("count")) 
    quarterly_frequency.append(group_frequency)
    
# Print the output for each quarterly period
for idx, period_frequency in enumerate(quarterly_frequency):
    period_title = quarterly_groups[idx][0] + " to " + quarterly_groups[idx][1]
    print(f"Word frequencies for period: {period_title}")
    if period_frequency.count() > 0:
        period_frequency.orderBy(desc("count")).limit(15).show(truncate=False) 
    else:
        print("No data for this period")
'''
# Create an empty DataFrame to hold the consolidated output
consolidated_output = spark.createDataFrame([], schema=quarterly_frequency[0].schema)

# Append the top ten word frequencies for each quarterly period to the consolidated output
for period_frequency in quarterly_frequency:
    top_ten = period_frequency.orderBy(desc("count")).limit(15) 
    consolidated_output = consolidated_output.union(top_ten)

# Save the consolidated output to a CSV file
consolidated_output.coalesce(1).write.csv("/home/madkins/example/Afbpac_propub_mssgtrends2.csv", header=True)
'''