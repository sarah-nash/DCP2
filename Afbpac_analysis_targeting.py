'''
Word Count Analysis (Top 10)

Dataset | Column 
-------------------+
FBPAC   | targeting

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

stop_words = StopWordsRemover().getStopWords() + ["classbl", "ad", "reach", "es", "classm", "classi", "seeing", "youre",
                                                  "youve", "b", "wants", "based", "href", "information", "bpeople", "may",
                                                  "profile", "classc", "img", "class", "table", "prs", "au"]

df_targeting = df.select("targeting").na.drop()  
#print(df_ads)
#df_targeting.show(3, truncate = False)

# remove special characters and lower the case
df_cleaned_targeting = df.select("targeting").na.drop()

df_cleaned_targeting = df_cleaned_targeting.withColumn("cleaned_text", regexp_replace(df_cleaned_targeting.targeting, '<[^>]+>', '')) ## added

df_cleaned_targeting = df_cleaned_targeting.withColumn("cleaned_text", regexp_replace(df_cleaned_targeting.targeting, '[^a-zA-Z\\s]', ''))
df_cleaned_targeting = df_cleaned_targeting.withColumn("cleaned_text", lower(df_cleaned_targeting.cleaned_text))
#df_cleaned_ads.show()

# tokenize the text
tokenizer = Tokenizer(inputCol="cleaned_text", outputCol="words")
df_tokenized_targeting = tokenizer.transform(df_cleaned_targeting)
#df_tokenized_ads.show()

# remove stop words
remover = StopWordsRemover(inputCol="words", outputCol="filtered", stopWords=stop_words)
df_cleaned_targeting = remover.transform(df_tokenized_targeting)
#df_cleaned_ads.show()

# explode the filtered words
df_words_targeting = df_cleaned_targeting.select(explode("filtered").alias("word")).filter(col("word") != '').filter(col("word") != ' ') # added. 
#df_words_targeting.show()

# count frequency of each word
df_frequency_targeting = df_words_targeting.groupBy("word").count()

# sort the words in descending order of their frequency
df_sorted_targeting = df_frequency_targeting.sort(desc("count"))

# display the top 10 most common words
print("Word Frequency - targeting")
df_sorted_targeting.show(10)


df_sorted_targeting = df_sorted_targeting.limit(10)

output_path = "/home/madkins/example/fbpac_targeting.csv"

df_sorted_targeting.write.format("csv") \
    .option("header", "true") \
    .option("truncate", False) \
    .mode("overwrite") \
    .save(output_path)
