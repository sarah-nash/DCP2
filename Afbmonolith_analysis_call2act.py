import pyspark
import nltk
from nltk.corpus import stopwords
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, explode, regexp_replace, desc, lower
from pyspark.ml.feature import StopWordsRemover, Tokenizer

spark = SparkSession.builder.master("local[*]").appName("proj2tn").getOrCreate()
df = spark.read.csv("/home/madkins/teamnorth/data/fb_monolith.csv",
				inferSchema = True,
				header = True,
				sep = ",",
				quote = "",
				escape = "",
				multiLine = True)

#df.show(1)

nltk.download('stopwords')
stop_words = stopwords.words('english')
stop_words = StopWordsRemover().getStopWords() 

df_call2act = df.select(" call_to_action").na.drop()
df_call2act.show(3, truncate = False)
df_call2act = df_call2act.withColumn("cleaned_text", regexp_replace(df_call2act[" call_to_action"], '<[^>]+>', ''))
df_call2act = df_call2act.withColumn("cleaned_text", regexp_replace(df_call2act[" call_to_action"], '[^a-zA-Z\\s]', ''))
df_call2act = df_call2act.withColumn("cleaned_text", lower(df_call2act.cleaned_text))

tokenizer = Tokenizer(inputCol="cleaned_text", outputCol="words")
df_tokenized_call2act = tokenizer.transform(df_call2act)

remover = StopWordsRemover(inputCol="words", outputCol="filtered", stopWords=stop_words)
df_call2act = remover.transform(df_tokenized_call2act)

df_words_call2act = df_call2act.select(explode("filtered").alias("word")).filter(col("word") != '')

df_freq_call2act = df_words_call2act.groupBy("word").count()

df_sorted_call2act = df_freq_call2act.sort(desc("count"))

print("Word Frequency - call_to_action")
df_sorted_call2act.show(10)
#df.printSchema()

df_sorted_call2act = df_sorted_call2act.limit(10)

output_path = "/home/madkins/example/Afbmonolith_call2act.csv"

# write the output DataFrame to a CSV file
df_sorted_call2act.write.format("csv").option("header", "true").mode("overwrite").save(output_path)
