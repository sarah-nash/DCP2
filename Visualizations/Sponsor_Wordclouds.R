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

# Top20 Sponsor clouds for all years:
#       pb_top20_sponsors_cloud   
#      mono_top20_sponsors_cloud
#                                   See Below to change the number of words for these clouds
# Pure Sponsor Clouds by Year: 
#       pb_2017_sponsors_cloud
#       pb_2018_sponsors_cloud
#       pb_2019_sponsors_cloud
#       mono_2020_sponsors_cloud
#       mono_2021_sponsors_cloud
#       mono_2022_sponsors_cloud

##########################################################################################


################## Sponsor Clouds ###############
# ---------------- Pure Clouds --------------
# Top 20 sponsors for pb
pb_top20_sponsors_cloud <- wordcloud2(pb_top20_pages)

# Top 20 sponsors for monolith
mono_top20_sponsors_cloud <- wordcloud2(mono_top20_pages)

# ------------Yearly Sponsors --------------
# 2017
pb_2017_sponsors <- pb2017_pages %>%
  ungroup() %>%
  select(title, n)

# pb_2017_sponsors <- head(pb_2017_sponsors , 30)     #<<<<<<--- Use this code to change number of words in cloud

pb_2017_sponsors_cloud <- wordcloud2(pb_2017_sponsors)

#2018
pb_2018_sponsors <- pb2018_pages %>%
  ungroup() %>%
  select(title, n)

# pb_2018_sponsors <- head(pb_2018_sponsors , 30)

pb_2018_sponsors_cloud <- wordcloud2(pb_2018_sponsors)

#2019
pb_2019_sponsors <- pb2019_pages %>%
  ungroup() %>%
  select(title, n)

# pb_2019_sponsors <- head(pb_2019_sponsors , 30)

pb_2019_sponsors_cloud <- wordcloud2(pb_2019_sponsors)

#2020
mono_2020_sponsors <- mono2020_pages %>%
  ungroup() %>%
  select(page_name, n)

# mono_2020_sponsors <- head(mono_2020_sponsors , 30)

mono_2020_sponsors_cloud <- wordcloud2(mono_2020_sponsors)

#2021
mono_2021_sponsors <- mono2021_pages %>%
  ungroup() %>%
  select(page_name, n)

# mono_2021_sponsors <- head(mono_2021_sponsors , 30)

mono_2021_sponsors_cloud <- wordcloud2(mono_2021_sponsors)

#2022
mono_2022_sponsors <- mono2022_pages %>%
  ungroup() %>%
  select(page_name, n)

# mono_2022_sponsors <- head(mono_2022_sponsors , 30)

mono_2022_sponsors_cloud <- wordcloud2(mono_2022_sponsors)
