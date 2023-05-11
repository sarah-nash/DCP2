'''
Word Count Analysis (Top 10)

Dataset | Column 
----------------+
FBPAC   | Message

This script was applied to the fbpac dataset in the teamnorth folder. In my analysis, I found some counts for nonwords that
had to be filtered out. Those nonwords are located in the list, line 33. 

'''

import pyspark
import nltk
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, regexp_replace, desc, lower, col, count
from pyspark.ml.feature import StopWordsRemover, Tokenizer
from nltk.corpus import stopwords

spark = SparkSession.builder.master("local[*]").appName("project2init").getOrCreate()
# /home/madkins/data/new_sample.csv
df = spark.read.csv("/home/madkins/teamnorth/data/fbpac-ads-en-US.csv", 
				inferSchema = True,
				header = True,
				sep = ',',
				#quote = "",
				escape = '"',
				multiLine = True)

nltk.download('stopwords')
stop_words = stopwords.words('english')

stop_words = StopWordsRemover().getStopWords() + ["us", "help", "people", "need", "make", "get", "span", "time", "like", 
                                                  "today", "take", "right", "afzspanspan", "classafxspan", "classcl",
                                                  "new", "classmfrspan", "one", "donate", "join", "back", "please", "every",
                                                  "day", "know", "care", "sign", "classcnspan", "pthe", "gift", "htz"]

df_mssg = df.select("message").na.drop()  
#print(df_ads)
#df_ads.show()

# remove special characters and lower the case
df_cleaned_mssg = df.select("message").na.drop()

df_cleaned_mssg = df_cleaned_mssg.withColumn("cleaned_text", regexp_replace(df_cleaned_mssg.message, '<[^>]+>', '')) ## added

df_cleaned_mssg = df_cleaned_mssg.withColumn("cleaned_text", regexp_replace(df_cleaned_mssg.message, '[^a-zA-Z\\s]', ''))
df_cleaned_mssg = df_cleaned_mssg.withColumn("cleaned_text", lower(df_cleaned_mssg.cleaned_text))
#df_cleaned_ads.show()

# tokenize the text
tokenizer = Tokenizer(inputCol="cleaned_text", outputCol="words")
df_tokenized_mssg = tokenizer.transform(df_cleaned_mssg)
#df_tokenized_ads.show()

# remove stop words
remover = StopWordsRemover(inputCol="words", outputCol="filtered", stopWords=stop_words)
df_cleaned_mssg = remover.transform(df_tokenized_mssg)
#df_cleaned_ads.show()

# explode the filtered words
df_words_mssg = df_cleaned_mssg.select(explode("filtered").alias("word")).filter(col("word") != '').filter(col("word") != ' ') # added. 
#df_words_mssg.show()

# count frequency of each word
df_frequency_mssg = df_words_mssg.groupBy("word").count()

# sort the words in descending order of their frequency
df_sorted_mssg = df_frequency_mssg.sort(desc("count"))

# display the top 10 most common words
print("Word Frequency - Message")
df_sorted_mssg.show(10)

'''
df_sorted_mssg = df_sorted_mssg.limit(10)

output_path = "/home/madkins/example/ppcleaned_mssg.csv"

df_sorted_mssg.write.format("csv") \
    .option("header", "true") \
    .option("truncate", False) \
    .mode("overwrite") \
    .save(output_path)
'''
# Most Common Words in the Sample 
# Example
#df_ads.show(3, truncate=False)
#df_ads.show(1)
