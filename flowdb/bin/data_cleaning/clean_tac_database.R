#
# Cleans TAC database.
#
# Author: Luis Capelo <luis.capelo@flowminder.org>
#
library(readr)
library(dplyr)

tac <- read_csv('data/tac_database.csv')
tac <- filter(tac, hnd_type != 'Basic')

filter_duplicates <- function(database) {
  df <- tac[!duplicated(tac$id), ]
  return(df)
}

tac <- filter_duplicates(tac)

write_csv(tac, 'data/tac_database.csv')
