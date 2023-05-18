library(dplyr)
library(ggplot2)
library(lubridate)
library(wordcloud2)
library(wordcloud)
library(tidyverse)
library(tidytext)
library(tm)
library(forcats)

##########################  Notes on this File #############################

# This file contains unused analysis of trends in political-ness among the ad data over time.
# Unfortunately the political values within the data did not seem to accurately represent any sort of 
# meaningful information about the ads. It would be best to perform a separate analysis on political
# value within these ads and then use this code.

##########################################################################################

################# Ad distributions ###############
#--------------- Monolith ---------------------
# Violin plots
mono_monthly_political_violin <- ggplot(monolith_time, aes(x = month, y = political_value, fill = month)) + 
  geom_violin() +
  theme(legend.position = "none")

mono20_monthly_political_violin <- ggplot(mono2020, aes(x = month, y = political_value, fill = month)) + 
  geom_violin()

mono21_monthly_political_violin <- ggplot(mono2021, aes(x = month, y = political_value, fill = month)) + 
  geom_violin() +
  theme(legend.position = "none")

mono22_monthly_political_violin <- ggplot(mono2022, aes(x = month, y = political_value, fill = month)) + 
  geom_violin()

# Line plots - triple axis?
mono_line <- ggplot(mono_average_daily_political_value, aes(x = year_month_day, y = average_political)) + 
  geom_line()

mono20_line <- ggplot(mono20_average_daily_political_value, aes(x = year_month_day, y = average_political)) + 
  geom_line()

mono21_line <- ggplot(mono21_average_daily_political_value, aes(x = year_month_day, y = average_political)) + 
  geom_line()

mono22_line <- ggplot(mono22_average_daily_political_value, aes(x = year_month_day, y = average_political)) + 
  geom_line()
