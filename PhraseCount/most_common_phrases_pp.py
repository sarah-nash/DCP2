import csv
import pandas as pd
import functools as ft
import advertools as at

PHRASE_LENGTH = 2

# read message / ad_text column into list
file_name = 'pro_publica_sample_raw.csv' 
f = open(file_name)
csv_file = csv.reader(f)
list_pp = []
for line in csv_file:
    list_pp.append(line[5]) 

# create list of lists of phrases
tokens_pp = at.word_tokenize(list_pp, phrase_len=PHRASE_LENGTH)

# flatten list
flattened_tokens_pp = ft.reduce(lambda a,b:a+b, tokens_pp)

# count most common phrases
most_common_tokens_pp = pd.Series(flattened_tokens_pp).value_counts()
print(most_common_tokens_pp[:5])








# df method if needed for some reason

# # read csv into pandas dataframe
# df_pp = pd.read_csv('pro_publica_sample_raw.csv')

# # convert dataframe to list
# list_pp = list(df_pp['message'].values)
