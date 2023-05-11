'''
Word Count Analysis (Top 10)

Dataset | Column 
-------------------+
FBPAC   | entities

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

stop_words = StopWordsRemover().getStopWords() 

df_entities = df.select("entities").na.drop()  
#print(df_ads)
df_entities.show(3, truncate = False)

# remove special characters and lower the case
df_cleaned_entities = df.select("entities").na.drop()

df_cleaned_entities = df_cleaned_entities.withColumn("cleaned_text", regexp_replace(df_cleaned_entities.entities, '<[^>]+>', '')) ## added

df_cleaned_entities = df_cleaned_entities.withColumn("cleaned_text", regexp_replace(df_cleaned_entities.entities, '[^a-zA-Z\\s]', ''))
df_cleaned_entities = df_cleaned_entities.withColumn("cleaned_text", lower(df_cleaned_entities.cleaned_text))
#df_cleaned_ads.show()

# tokenize the text
tokenizer = Tokenizer(inputCol="cleaned_text", outputCol="words")
df_tokenized_entities = tokenizer.transform(df_cleaned_entities)
#df_tokenized_ads.show()

# remove stop words
remover = StopWordsRemover(inputCol="words", outputCol="filtered", stopWords=stop_words)
df_cleaned_entities = remover.transform(df_tokenized_entities)
#df_cleaned_ads.show()

# explode the filtered words
df_words_entities = df_cleaned_entities.select(explode("filtered").alias("word")).filter(col("word") != '').filter(col("word") != ' ') # added. 
#df_words_entities.show()

# count frequency of each word
df_frequency_entities = df_words_entities.groupBy("word").count()

# sort the words in descending order of their frequency
df_sorted_entities = df_frequency_entities.sort(desc("count"))

# display the top 10 most common words
print("Word Frequency - entities")
df_sorted_entities.show(10)


df_sorted_entities = df_sorted_entities.limit(10)

output_path = "/home/madkins/example/fbpac_entities.csv"

df_sorted_entities.write.format("csv") \
    .option("header", "true") \
    .option("truncate", False) \
    .mode("overwrite") \
    .save(output_path)
