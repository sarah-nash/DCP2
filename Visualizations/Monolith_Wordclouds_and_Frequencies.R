library(dplyr)
library(ggplot2)
library(lubridate)
library(wordcloud2)
library(wordcloud)
library(tidyverse)
library(tidytext)
library(tm)
library(forcats)

##########################  Recommended Objects in this File #############################
#         ****   You can call any of these by highlighting one, then Shift + Enter   ****

# mono_wdcloud #
# This is a pure wordcloud (all words).
# You can choose the year you want by adding the year suffix to the object: 20, 21, or 22


# mono_top20_wdcloud_filtered #
# This is the top 20 wordcloud.
# You can change the number of words you want to see by changing the following code:
#         mono_top20_filtered <- head(mono_freq_df_filtered, 20) #<- 20 is the default number of words
#  and reloading the wordcloud:
#         mono_top20_wdcloud_filtered <- wordcloud2(mono_top20_filtered)

# There are yearly variations for these top20 clouds too: # top20_wdcloud20 #
# You can change the number of words you want to see by changing the following code:
#         top20_df20 <- head(freq_df20, 20) #<- 20 is the default number of words
#  and reloading the wordcloud:
#         top20_wdcloud20 <- wordcloud2(top20_df20)
# You can choose the year you want by adding the year suffix to the objects (instead of 20): 20, 21, or 22

# There are monthly variations for these clouds as well. Scroll to the very bottom of the file to view and
# edit them.

### Histograms of Word Frequencies ###
#       mono_top20_hist_filtered       #
#             top20_hist20           #
#             top20_hist21           #
#             top20_hist22           #
#          top20_monthly_hist        #

##########################################################################################



#### all word clouds over time/in different times #####
mono_ungrouped <- monolith_time %>%
  ungroup()


######################## Ad Text Word Cloud ##########################
#-------------------- Monolith Ad text -----------------------------
#create stopwords DF
eng_stopwords = data.frame(word = stopwords("eng"))

# - anti_join  - function to remove stopwords
new_df <- mono_ungrouped %>%
  unnest_tokens(word, ad_text) %>%
  anti_join(eng_stopwords)

# Word Frequencies
freq_df <- new_df %>%
  count(word) %>%
  arrange(desc(n))

#--------------- Filtering words out ---------------
mono_freq_df_filtered <- freq_df %>%
  filter(word != "comment",
         word != "like",
         word != "facebook",
         word != "https",
         word != "1")

mono_top20_filtered <- head(mono_freq_df_filtered, 20)

mono_top20_hist_filtered <-
  ggplot(mono_top20_filtered, aes(
    x = fct_reorder(word, n),
    y = n,
    fill = word
  )) +
  geom_col() +
  theme(legend.position = "none") +
  labs(x = "Word", y = "Count", title = "Top 20 Filtered Words in Facebook Monolith's Ad Texts")


# Pure WordCloud
mono_wdcloud <- wordcloud2(mono_freq_df_filtered)

# Top20 WordCloud
mono_top20_wdcloud_filtered <- wordcloud2(mono_top20_filtered)


#------------------ Filtering by Years -------------------
#-------- 2020 ----------
# Word Frequencies
freq_df20 <- new_df %>%
  filter(
    year == "2020",
    word != "comment",
    word != "like",
    word != "facebook",
    word != "https",
    word != "1"
  ) %>%
  count(word) %>%
  arrange(desc(n))

# Top 20 Words
top20_df20 <- head(freq_df20, 20)

# Top20 Histogram
top20_hist20 <-
  ggplot(top20_df20, aes(
    x = fct_reorder(word, n),
    y = n,
    fill = word
  )) +
  geom_col() +
  theme(legend.position = "none") +
  labs(x = "Word", y = "Count", title = "Top 20 Words in Facebook Monolith's Ad Texts in 2020")

# Top50 WordCloud
mono_wdcloud20 <- wordcloud2(freq_df20)

# Top20 WordCloud
top20_wdcloud20 <- wordcloud2(top20_df20)



#-------- 2021 ----------
# Word Frequencies
freq_df21 <- new_df %>%
  filter(
    year == "2021",
    word != "comment",
    word != "like",
    word != "facebook",
    word != "https",
    word != "1"
  ) %>%
  count(word) %>%
  arrange(desc(n))

# Top 20 Words
top20_df21 <- head(freq_df21, 20)

# Top20 Histogram
top20_hist21 <-
  ggplot(top20_df21, aes(
    x = fct_reorder(word, n),
    y = n,
    fill = word
  )) +
  geom_col() +
  theme(legend.position = "none") +
  labs(x = "Word", y = "Count", title = "Top 21 Words in Facebook Monolith's Ad Texts in 2021")

# Top50 WordCloud
mono_wdcloud21 <- wordcloud2(freq_df21)

# Top20 WordCloud
top20_wdcloud21 <- wordcloud2(top20_df21)

#-------- 2022 ----------
# Word Frequencies
freq_df22 <- new_df %>%
  filter(
    year == "2022",
    word != "comment",
    word != "like",
    word != "facebook",
    word != "https",
    word != "1"
  ) %>%
  count(word) %>%
  arrange(desc(n))

# Top 20 Words
top20_df22 <- head(freq_df22, 20)

# Top20 Histogram
top20_hist22 <-
  ggplot(top20_df22, aes(
    x = fct_reorder(word, n),
    y = n,
    fill = word
  )) +
  geom_col() +
  theme(legend.position = "none") +
  labs(x = "Word", y = "Count", title = "Top 22 Words in Facebook Monolith's Ad Texts in 2022")

# Top50 WordCloud
mono_wdcloud22 <- wordcloud2(freq_df22)

# Top20 WordCloud
top20_wdcloud22 <- wordcloud2(top20_df22)

#---------- Months ------------
# Word Frequencies
freq_df_monthly <- new_df %>%
  group_by(month) %>%
  filter(word != "comment",
         word != "like",
         word != "facebook",
         word != "https",
         word != "1") %>%
  count(word) %>%
  arrange(desc(month), desc(n))

# Top 20 Words
top20_df_monthly <- freq_df_monthly %>%
  slice_max(order_by = n, n = 20)

# # # # # Define which month you want to look at here # # # # #

top20_df_monthly <- top20_df_monthly %>%
  ungroup() %>%
  filter(month == "01") %>%
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

# Top50 monthly WordCloud
top50_df_monthly <- freq_df_monthly %>%
  slice_max(order_by = n, n = 50)

top50_df_monthly <- top50_df_monthly %>%
  ungroup() %>%
  filter(month == "01") %>%
  select(!month)

top50_wdcloud_monthly <- wordcloud2(top50_df_monthly)
