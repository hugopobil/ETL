
# Hugo Pasqual del Pobil
# Pratica AIRBNB ETL

library(dplyr)
library(stringr)
conn <- DBI::dbConnect(RSQLite::SQLite(), dbname='airbnb.sqlite')

# Full tables to search variables
# Not part of the exercise
listings <- tbl(conn, sql("SELECT *
                          FROM Listings"))
# Visualize
listings

reviews <- tbl(conn, sql("SELECT *
                          FROM Reviews"))
# Visualize
reviews

# -------------------------------------------------------------------------
# 1. Extract Listings
# Select the columns we use

listings_e <- tbl(conn, sql(
"
SELECT L.id, L.name, L.room_type, H.neighbourhood_group, L.price, L.review_scores_rating, L.number_of_reviews
FROM Listings as L
INNER JOIN Hoods as H
  ON L.neighbourhood_cleansed = H.neighbourhood
"
))

listings<- data.frame(listings_e)
head(listings)

# -------------------------------------------------------------------------
# 2. Extract Reviews
reviews_e <- tbl(conn, sql(
"
SELECT R.listing_id, R.reviewer_id, R.reviewer_name, strftime('%Y-%m', R.date) AS mes, H.neighbourhood_group, count(R.id) as review_num
FROM Reviews as R 
INNER JOIN Listings as L 
  ON R.listing_id = L.id 
INNER JOIN Hoods as H 
  ON L.neighbourhood_cleansed = H.neighbourhood 
WHERE CAST(strftime('%Y', R.date) AS INT) > 2010 
GROUP BY mes, H.neighbourhood_group
"
))

reviews <- data.frame(reviews_e)
head(reviews)

# -------------------------------------------------------------------------
# 3. Transform Listings
listings <- listings %>% 
  mutate(price = as.numeric(str_replace_all(price, "\\$|\\,", "")))

listings

# -------------------------------------------------------------------------
# 4. Transform Listings (Option A)
listings_clean <- listings %>% 
  mutate(number_of_reviews = coalesce(number_of_reviews, 
                                      mean(number_of_reviews, 
                                           na.rm = TRUE))) %>% 
  mutate(review_scores_rating = coalesce(review_scores_rating, 
                                         mean(review_scores_rating, 
                                              na.rm = TRUE)))
listings_clean

# -------------------------------------------------------------------------
# 5. Transform Listings
transform_listings_5 <- listings_clean %>%
  group_by(neighbourhood_group, room_type) %>%
  summarise(weighted_mean = sum(review_scores_rating * number_of_reviews)/sum(number_of_reviews),
            median_price = median(price))

transform_listings_5

# -------------------------------------------------------------------------
# 6. Transform Reviews
head(reviews)
prediction <- reviews %>% 
  filter(mes == '2021-07')
  
prediction <- prediction %>% 
  mutate(mes = '2021-08')

reviews_with_prediction <- bind_rows(reviews, prediction)

# -------------------------------------------------------------------------
# 8. Load
DBI::dbWriteTable(conn=conn, 'transform_listings_5', transform_listings_5, overwrite=TRUE)
DBI::dbWriteTable(conn=conn, 'reviews_with_prediction', reviews_with_prediction, overwrite=TRUE)
DBI::dbWriteTable(conn=conn, 'listings_clean', listings_clean, overwrite=TRUE)

# Check the tables method 1 with no data extraction
DBI::dbListTables(conn)

# Check tables are uploaded correctly by downloading them
tbl(conn, sql("SELECT *
              FROM transform_listings_5 LIMIT 10"))

tbl(conn, sql("SELECT *
              FROM reviews_with_prediction LIMIT 10"))

tbl(conn, sql("SELECT *
              FROM listings_clean LIMIT 10"))








