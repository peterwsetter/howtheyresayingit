# Tokenize & Identify Quotes
# In this version, attempt to use str_extract_all

library(dplyr)
library(tidytext)
library(DBI)
library(stringr)

dbdir <- "../../data/bbcdb"

con <- dbConnect(MonetDBLite::MonetDBLite(), dbdir)

# Pull paragraphs into working memory
paragraphs <- tbl(con, 'article_content') %>% 
  collect() 

quote_regex <- '"[:alpha:].*[[:alpha:]|[:punct:]]".{0,1}'

paragraphs %>% 
  unnest_tokens(output = sentence_text,
                input = paragraph_text,
                token = 'sentences',
                to_lower = FALSE) %>% 
  group_by(article_id, paragraph_num) %>% 
  mutate(sentence_num = row_number()) %>% 
  ungroup() %>% 
  mutate(
         quote = str_extract_all(sentence_text, quote_regex, simplify = TRUE),
         nonquote = str_replace_all(sentence_text, quote_regex, '')
         ) %>%
  select(-sentence_text) ->
  quotes_separated

dbWriteTable(con, 'quotes_separated', quotes_separated)

dbDisconnect(con, shutdown=TRUE)
