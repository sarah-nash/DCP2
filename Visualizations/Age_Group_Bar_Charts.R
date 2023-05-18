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
#         ****   You can call any of these by highlighting one here, then Shift + Enter   ****

# mono_age_barplot  and  pb_age_barplot - Age distributions
# mono_age_props_barplot  and  pb_age_props_barplot - Age proportions

##########################################################################################


################# Barplots ###############
#-------------- Monolith Age Groups ----------------
mono_age_barplot <-
  ggplot(mono_age_groups, aes(x = rownames(mono_age_groups), y = t.mono_age_groups., fill = rownames(mono_age_groups))) +
  geom_bar(stat = "identity") +
  theme(legend.position = "none") + 
  labs(title = "Age Group Distributions According to FbMonolith (2020 - 2022)",
       subtitle = "Note: Some ads have more than 1 target group",
       x = "Age Group",
       y = "Number of Ads")

mono_age_props_barplot <-
  ggplot(mono_age_groups, aes(x = rownames(mono_age_groups), y = props, fill = rownames(mono_age_groups))) +
  geom_bar(stat = "identity") +
  theme(legend.position = "none") + 
  labs(title = "Age Group Proportional Distributions According to FbMonolith (2020 - 2022)",
       subtitle = "Note: Some ads have more than 1 target group; proportions will not sum to 1",
       x = "Age Group",
       y = "Proportion of Ads")

#-------------- Propublica Age Groups ---------------
pb_age_barplot <-
  ggplot(pb_age_groups, aes(x = rownames(pb_age_groups), y = t.pb_age_groups., fill =rownames(pb_age_groups))) +
  geom_bar(stat = "identity") +
  theme(legend.position = "none") +
  labs(title = "Age Group Distributions of Ads According to ProPublica (2017 - 2019)",
       subtitle = "Note: Some ads have more than 1 target group",
       caption = "*numbers are lower due to frequency of missing data (NA)",
       x = "Age Group",
       y = "Number of Ads")

pb_age_props_barplot <-
  ggplot(pb_age_groups, aes(x = rownames(pb_age_groups), y = props, fill = rownames(pb_age_groups))) +
  geom_bar(stat = "identity") +
  theme(legend.position = "none") + 
  labs(title = "Age Group Proportional Distributions According to ProPublica (2017 - 2019)",
       subtitle = "Note: Some ads have more than 1 target group; proportions will not sum to 1",
       caption = "*numbers are lower due to frequency of missing data (NA)",
       x = "Age Group",
       y = "Proportion of Ads")
