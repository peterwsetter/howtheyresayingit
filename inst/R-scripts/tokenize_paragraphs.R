# Tokenize & Identify Quotes

library(dplyr)
library(tidytext)
library(DBI)

dbdir <- "../../data/bbcdb"

con <- dbConnect(MonetDBLite::MonetDBLite(), dbdir)

# Pull paragraphs into working memory
paragraphs <- tbl(con, 'article_content') %>% 
  collect() 

data(stop_words)

# Tokenize paragraphs into individual words
# Since we want to identify where quotes begin and end
#  we'll break on spaces.
paragraphs %>% 
  # See unnest_token documentation for this example
  unnest_tokens(output = word_raw, 
                input = paragraph_text,
                token = stringr::str_split,
                pattern = ' ',
                to_lower = FALSE
                ) %>% 
  group_by(article_id, paragraph_num) %>% 
  mutate(
    # Add word order source: https://stackoverflow.com/a/12925090
    word_num = row_number(),
    # Identify word as start or end of quote
    start_end_quote = case_when(
      stringr::str_detect(word_raw, '^"') ~ 'start',
      # The .{0,1} accounts for puncuation after the quote
      stringr::str_detect(word_raw, '".{0,1}$') ~ 'end',
      TRUE ~ 'other'
    )) %>% 
  ungroup() %>% 
  mutate(
    # Identify whether a row is quoted based on nearby words
    quoted_raw = case_when(
      start_end_quote %in% c('start', 'end') ~ 1L,
      lead(start_end_quote) == 'start' ~ 0L, # Allows look ahead to first quote
      lag(start_end_quote) == 'end' ~ 0L, 
      row_number() == n() ~ 0L, # Assign last row
      TRUE ~ NA_integer_
    ),
    # Fill in missing values.
    # fromLast = TRUE == looking ahead to fill in a value
    quoted = zoo::na.locf(quoted_raw, fromLast = TRUE)
    ) %>%
  # Cleanup the dataset, removing puncuation and stop words
  mutate(word = tm::removePunctuation(word_raw,
                                      preserve_intra_word_contractions = TRUE),
         word_lower = tolower(word)
         ) %>% 
  anti_join(stop_words,
            by = c('word_lower' = 'word')) %>% 
  # Preserve captialized version of word
  select(article_id, paragraph_num, word_num, word, word_lower, quoted) ->
  article_tokenized

dbWriteTable(con, 'article_tokenized', article_tokenized)

dbDisconnect(con, shutdown=TRUE)
