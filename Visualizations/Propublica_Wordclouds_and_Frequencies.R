library(dplyr)
library(ggplot2)
library(lubridate)
library(wordcloud2)
library(wordcloud)
library(tidyverse)
library(tidytext)
library(tm)
library(forcats)
library(stringr)
##########################  Recommended Objects in this File #############################
#         ****   You can call any of these by highlighting one, then Shift + Enter   ****

# pb_wdcloud #
# This is a pure wordcloud (all words). 
# You can choose the year you want by adding the year suffix to the object: 17, 18, or 19


# pb_top20_filtered_wdcloud #
# This is the top 20 wordcloud.
# You can change the number of words you want to see by changing the following code and reloading the wordcloud::
# pb_top20_filtered <- head(pb_freq_df_filtered, 20) #<- 20 is the default number of words

# There are yearly variations for these top20 clouds too: # pb_wdcloud17 #
# You can change the number of words you want to see by changing the following code and reloading the wordcloud::
# top20_17 <- head(freq_df17, 20) #<- 20 is the default number of words
# You can choose the year you want by adding the year suffix to the object: 17, 18, or 19

# There are monthly variations for these clouds as well. Scroll to the very bottom of the file to view and
# edit them.

### Histograms of Word Frequencies ###
#       pb_top20_hist_filtered       #
#             top20_hist17           #
#             top20_hist18           #
#             top20_hist19           #
#          top20_monthly_hist        #

##########################################################################################



#### all word clouds over time/in different times #####
pb_ungrouped <- pb_time %>%
  ungroup()



######################## Ad Text Word Cloud ##########################
# ------------------------- Propublica Ad Text  -----------------------
#create stopwords DF
eng_stopwords = data.frame(word = stopwords("eng"))

# - anti_join  - function to remove stopwords
new_df <- pb_ungrouped %>%
  unnest_tokens(word, message) %>%
  anti_join(eng_stopwords)

# Word Frequencies
freq_df <- new_df %>%
  count(word) %>%
  arrange(desc(n))

# Filtering words out
pb_freq_df_filtered <-
  freq_df %>%  # <----- This code filters out words we don't like
  filter(
    word != "comment",
    word != "like",
    word != "facebook",
    word != "https",
    word != "1",
    word != "p",
    word != "class",
    word != "span",
    word != "div",
    word != "https",
    word != "href",
    word != "will",
    word != "www.facebook.com",
    word != "id",
    word != "img",
    word != "br",
    word != "_5mfr",
    word != "bit.ly",
    word != "http",
    !str_detect(word, "_5")
  )

pb_top20_filtered <- head(pb_freq_df_filtered, 20)# <<<<<<--- Use this code to change number of words in cloud

pb_top20_hist_filtered <-
  ggplot(pb_top20_filtered, aes(
    x = fct_reorder(word, n),
    y = n,
    fill = word
  )) +
  geom_col() +
  theme(legend.position = "none") +
  labs(x = "Word", y = "Count", title = "Top 20 Filtered Words in Propublica's Ad Texts")

# Pure WordCloud
pb_wdcloud <- wordcloud2(freq_df_filtered)

# Top20 WordCloud
pb_top20_filtered_wdcloud <- wordcloud2(pb_top20_filtered)

#------------------ Filtering by Timestamps -------------------
#-------- 2017 ----------
# Word Frequencies
freq_df17 <- new_df %>%
  filter(
    year == "2017",
    word != "comment",
    word != "like",
    word != "facebook",
    word != "https",
    word != "1",
    word != "p",
    word != "class",
    word != "span",
    word != "div",
    word != "https",
    word != "href",
    word != "will",
    word != "www.facebook.com",
    word != "id",
    word != "img",
    word != "br",
    word != "_5mfr",
    word != "bit.ly",
    word != "http",
    !str_detect(word, "_")
  ) %>%
  count(word) %>%
  arrange(desc(n))

# Top 20 Words
top20_17 <- head(freq_df17, 20)

# Top20 Histogram
top20_hist17 <-
  ggplot(top20_17, aes(
    x = fct_reorder(word, n),
    y = n,
    fill = word
  )) +
  geom_col() +
  theme(legend.position = "none") +
  labs(x = "Word", y = "Count", title = "Top 20 Words in Propublica's Ad Texts in 2017")

# Pure WordCloud
pb_wdcloud17 <- wordcloud2(freq_df17)

# Top20 WordCloud
top20_17_wdcloud <- wordcloud2(top20_17)

#-------- 2018 ----------
# Word Frequencies
freq_df18 <- new_df %>%
  filter(
    year == "2018",
    word != "comment",
    word != "like",
    word != "facebook",
    word != "https",
    word != "1",
    word != "p",
    word != "class",
    word != "span",
    word != "div",
    word != "https",
    word != "href",
    word != "will",
    word != "www.facebook.com",
    word != "id",
    word != "img",
    word != "br",
    word != "_5mfr",
    word != "bit.ly",
    word != "http",
    !str_detect(word, "_")
  ) %>%
  count(word) %>%
  arrange(desc(n))

# Top 20 Words
top20_18 <- head(freq_df18, 20)

# Top20 Histogram
top20_hist18 <-
  ggplot(top20_18, aes(
    x = fct_reorder(word, n),
    y = n,
    fill = word
  )) +
  geom_col() +
  theme(legend.position = "none") +
  labs(x = "Word", y = "Count", title = "Top 20 Words in Propublica's Ad Texts in 2018")

# Pure WordCloud
pb_wdcloud18 <- wordcloud2(freq_df18)

# Top20 WordCloud
top20_18_wdcloud <- wordcloud2(top20_18)

#-------- 2019 ----------
# Word Frequencies
freq_df19 <- new_df %>%
  filter(
    year == "2019",
    word != "comment",
    word != "like",
    word != "facebook",
    word != "https",
    word != "1",
    word != "p",
    word != "class",
    word != "span",
    word != "div",
    word != "https",
    word != "href",
    word != "will",
    word != "www.facebook.com",
    word != "id",
    word != "img",
    word != "br",
    word != "_5mfr",
    word != "bit.ly",
    word != "http",
    !str_detect(word, "_")
  ) %>%
  count(word) %>%
  arrange(desc(n))

# Top 22 Words
top20_19 <- head(freq_df19, 20)

# Top22 Histogram
top20_hist19 <-
  ggplot(top20_19, aes(
    x = fct_reorder(word, n),
    y = n,
    fill = word
  )) +
  geom_col() +
  theme(legend.position = "none") +
  labs(x = "Word", y = "Count", title = "Top 22 Words in Facebook Monolith's Ad Texts in 2019")

# Pure WordCloud
pb_wdcloud19 <- wordcloud2(freq_df19)

# Top22 WordCloud
top20_19_wdcloud <- wordcloud2(top20_19)

#----------------------------------- Months ------------------------------------
# Word Frequencies
freq_df_monthly <- new_df %>%
  group_by(month) %>%
  filter(
    word != "comment",
    word != "like",
    word != "facebook",
    word != "https",
    word != "1",
    word != "p",
    word != "class",
    word != "span",
    word != "div",
    word != "https",
    word != "href",
    word != "will",
    word != "www.facebook.com",
    word != "id",
    word != "img",
    word != "br",
    word != "_5mfr",
    word != "bit.ly",
    word != "http",
    !str_detect(word, "_5")
  ) %>%
  count(word) %>%
  arrange(desc(month), desc(n))

# Top 20 Words
top20_df_monthly <- freq_df_monthly %>%
  slice_max(order_by = n, n = 20) #<------------Change number of words you want per month here

# # # # # Define which month you want to look at here # # # # #
top20_df_monthly <- top20_df_monthly %>%
  ungroup() %>%
  filter(month == "01") %>% # "01" = January
  select(!month)
# ^^^^^^^^ Define which month you want to look at here ^^^^^ #

# Top20 Histogram
top20_monthly_hist <-
  ggplot(top20_df_monthly, aes(
    x = fct_reorder(word, n),
    y = n,
    fill = word
  )) +
  geom_col() +
  theme(legend.position = "none") +
  labs(x = "Word", y = "Count", title = "Top 20 Words in Facebook Monolith's Ad Texts in January")

# Top20 monthly WordCloud
top20_wdcloud_monthly <- wordcloud2(top20_df_monthly)

# Pure WordCloud
df_monthly <- freq_df_monthly %>%
  ungroup() %>%
  filter(month == "01") %>%
  select(!month)

pb_wdcloud_monthly <- wordcloud2(df_monthly) #January
