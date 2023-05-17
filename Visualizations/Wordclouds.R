library(dplyr)
library(ggplot2)
library(lubridate)
library(wordcloud2)
library(wordcloud)
library(tidyverse)
library(tidytext)
library(tm)
library(forcats)

View(new_df[1:500,])

#### all word clouds over time/in different times #####
mono_ungrouped <- monolith_time %>%
  ungroup()

######################## Word Cloud ##########################
#-------------------- Monolith -----------------------------
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

# Top 20 Words
top20_df <- head(freq_df, 20)

# Top20 Histogram
top20_hist <- ggplot(top20_df, aes(x = fct_reorder(word, n), y = n, fill = word)) + 
  geom_col() + 
  theme(legend.position = "none") +
  labs(x = "Word", y = "Count", title = "Top 20 Words in Facebook Monolith's Ad Texts")

# Top20 WordCloud
top20_wdcloud <- wordcloud2(top20_df)

# Top50 WordCloud
top50_df <- head(freq_df, 50)
top50_wdcloud <- wordcloud2(top50_df)
#--------------- Filtering words out ---------------
freq_df_filtered <- freq_df %>%
  filter(word != "comment",
         word != "like",
         word != "facebook",
         word != "https",
         word != "1")

top20_filtered <- head(freq_df_filtered, 20)

top20_hist_filtered <- ggplot(top20_filtered, aes(x = fct_reorder(word, n), y = n, fill = word)) + 
  geom_col() + 
  theme(legend.position = "none") +
  labs(x = "Word", y = "Count", title = "Top 20 Filtered Words in Facebook Monolith's Ad Texts")

# Top20 WordCloud
top20_wdcloud_filtered <- wordcloud2(top20_filtered)

# Top50 WordCloud
top50_df_filtered <- head(freq_df_filtered, 50)
top50_wdcloud <- wordcloud2(top50_df_filtered)

#------------------ Filtering by Timestamps -------------------
#-------- 2020 ----------
# Word Frequencies
freq_df20 <- new_df %>% 
  filter(year == "2020",
         word != "comment",
         word != "like",
         word != "facebook",
         word != "https",
         word != "1") %>%
  count(word) %>% 
  arrange(desc(n))

# Top 20 Words
top20_df20 <- head(freq_df20, 20)

# Top20 Histogram
top20_hist20 <- ggplot(top20_df20, aes(x = fct_reorder(word, n), y = n, fill = word)) + 
  geom_col() + 
  theme(legend.position = "none") +
  labs(x = "Word", y = "Count", title = "Top 20 Words in Facebook Monolith's Ad Texts in 2020")

# Top20 WordCloud
top20_wdcloud20 <- wordcloud2(top20_df20)

# Top50 WordCloud
top50_df20 <- head(freq_df20, 50)
top50_wdcloud20 <- wordcloud2(top50_df20)

#-------- 2021 ----------
# Word Frequencies
freq_df21 <- new_df %>% 
  filter(year == "2021",
         word != "comment",
         word != "like",
         word != "facebook",
         word != "https",
         word != "1") %>%
  count(word) %>% 
  arrange(desc(n))

# Top 21 Words
top20_df21 <- head(freq_df21, 20)

# Top21 Histogram
top20_hist21 <- ggplot(top20_df21, aes(x = fct_reorder(word, n), y = n, fill = word)) + 
  geom_col() + 
  theme(legend.position = "none") +
  labs(x = "Word", y = "Count", title = "Top 21 Words in Facebook Monolith's Ad Texts in 2021")

# Top21 WordCloud
top20_wdcloud21 <- wordcloud2(top20_df21)

# Top50 WordCloud
top50_df21 <- head(freq_df21, 50)
top50_wdcloud21 <- wordcloud2(top50_df21)

#-------- 2022 ----------
# Word Frequencies
freq_df22 <- new_df %>% 
  filter(year == "2022",
         word != "comment",
         word != "like",
         word != "facebook",
         word != "https",
         word != "1") %>%
  count(word) %>% 
  arrange(desc(n))

# Top 22 Words
top20_df22 <- head(freq_df22, 20)

# Top22 Histogram
top20_hist22 <- ggplot(top20_df22, aes(x = fct_reorder(word, n), y = n, fill = word)) + 
  geom_col() + 
  theme(legend.position = "none") +
  labs(x = "Word", y = "Count", title = "Top 22 Words in Facebook Monolith's Ad Texts in 2022")

# Top22 WordCloud
top20_wdcloud22 <- wordcloud2(top20_df22)

# Top50 WordCloud
top50_df22 <- head(freq_df22, 50)
top50_wdcloud22 <- wordcloud2(top50_df22)
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

View(top20_df_monthly)

# Define which month you want to look at here #

top20_df_monthly <- top20_df_monthly%>%
  ungroup() %>%
  filter(month == "01") %>%
  select(!month)

# Top20 Histogram
top20_monthly_hist <- ggplot(top20_df_monthly, aes(x = fct_reorder(word, n), y = n, fill = word)) + 
  geom_col() + 
  theme(legend.position = "none") +
  labs(x = "Word", y = "Count", title = "Top 20 Words in Facebook Monolith's Ad Texts in January")

# Top22 WordCloud
top20_wdcloud_monthly <- wordcloud2(top20_df_monthly)

# Top50 WordCloud
top50_df_monthly <- freq_df_monthly %>%
  slice_max(order_by = n, n = 50)

top50_df_monthly <- top50_df_monthly%>%
  ungroup() %>%
  filter(month == "01") %>%
  select(!month)

top50_wdcloud_monthly <- wordcloud2(top50_df_monthly)
