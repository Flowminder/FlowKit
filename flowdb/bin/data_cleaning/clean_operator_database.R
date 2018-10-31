#
# Cleaning operator database.
#
# Author: Luis Capelo <luis.capelo@flowminder.org>
#
library(readr)
library(dplyr)
library(countrycode)

operators <- read_csv('data/operator_database.csv')

#
#  Cleaning typos.
#
operators$Country <- ifelse(operators$Country == 'Czad', 'Chad', operators$Country)
operators$Country <- ifelse(operators$Country == 'Guiana', 'French Guiana', operators$Country)
operators$Country <- ifelse(operators$Country == 'Luksemburg', 'Luxembourg', operators$Country)
operators$Country <- ifelse(operators$Country == 'Mauretania', 'Mauritania', operators$Country)

#
#  Adding ISO 3-letter codes.
#
operators$ISO <- countrycode(operators$Country, 'country.name', 'iso3c')

#
#  Storing output.
#
write_csv(operators, 'data/operator_database.csv')
