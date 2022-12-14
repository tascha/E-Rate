print(Sys.time())

# Load libraries
library(dplyr)
library(tidyr)
library(stringr)
library(glue)
library(purrr)
library(fuzzyjoin)
library(geosphere)
library(janitor)
library(stringdist)
library(rqdatatable)
library(rquery)
library(janitor)
library(aws.s3)
library(aws.ec2metadata)

# Read in most recent Erate Commitments library data stored in S3
erate_libs <-
  s3read_using(FUN = read.csv,
               object = "Libraries_Funded_Committed_AVI8-SVP9.csv",
               bucket = "erate-data/data/AVI8-SVP9_Commitments")
erate_libs <- erate_libs %>% select(-X)

# Read in matched data stored in S3 (this was created during a previous run of this script)
matches <-
  s3read_using(FUN = read.csv,
               object = "USAC_IMLS_MATCHED.csv",
               bucket = "erate-data/data/USAC_IMLS_Match")
matches <- matches %>% 
  select(ros_entity_number, ros_physical_state, FSCSKEY, FSCS_SEQ) %>% 
  mutate(ros_physical_state = toupper(ros_physical_state))

# The following commented out code can be run if new items are added to the Hand_Matches data

# # Read in matched data stored in S3
# # Import hand matching dataset with ros_entity_numbers matched to FSCS keys
# hand_matches <-
#   s3read_using(
#     FUN = read.csv,
#     na.strings = c("", " ", "N/A", "n/a"),
#     object = "data/Hand_Matches.csv",
#     bucket = "erate-data"
#   )
# 
# hand_matches <- hand_matches %>% 
#   filter(!is.na(FSCSKEY) & !is.na(FSCS_SEQ)) %>% 
#   select(ros_entity_number, ros_physical_state, FSCSKEY, FSCS_SEQ) %>% 
#   mutate(ros_physical_state = toupper(ros_physical_state),
#          FSCS_SEQ = as.integer(FSCS_SEQ))
# 
# # Merge the hand_matches and matches
# # Remove duplicates
# matches <- matches %>%
#   full_join(hand_matches) %>% 
#   distinct(ros_entity_number, ros_physical_state, FSCSKEY, FSCS_SEQ)



# Read in IMLS PLS datasets stored in S3
imls <-
  s3read_using(FUN = read.csv,
               object = "data/IMLS_Unique_Entities_2014-2020.csv",
               bucket = "erate-data")
imls <- imls %>% select(-X)

# I'm now going to add FSCSKEY and FSCS_SEQ columns to erate_libs dataset and include the matches
erate_libs <- erate_libs %>%
  left_join(matches,
            by = c("ros_entity_number", "ros_physical_state")) %>%
  relocate(c("FSCSKEY", "FSCS_SEQ"), .after = ros_entity_number)

# Create a dataframe of erate libraries to use in matching
erate_libs_for_matching <- erate_libs %>%
  filter(is.na(FSCSKEY) & is.na(FSCS_SEQ)) %>%
  filter(
    ros_entity_number != 17012400,
    # this air force base lib doesn't match imls
    ros_entity_number != 137653,
    # this is a regional system that doesn't match imls
    ros_entity_number != 138097,
    # this is a regional system that doesn't match imls
    ros_entity_number != 137724,
    # this is a regional system that doesn't match imls
    ros_entity_number != 231108,
    # this is a regional system that doesn't match imls
    ros_entity_number != 16030444,
    # not an imls library
    ros_entity_number != 126021,
    # this is a regional system that doesn't match imls
    ros_entity_number != 17011387,
    # this is a regional system that doesn't match imls
    ros_entity_number != 16062292,
    # this is a library society that doesn't match imls
    ros_entity_number != 133460,
    # this is a regional system that doesn't match imls
    ros_entity_number != 17009767,
    # this is a federated system that doesn't match imls
  ) %>%
  # With distinct function, if there are multiple rows for a given combination of inputs,
  # only the first row will be preserved.
  distinct(ros_entity_number, ros_physical_state, .keep_all = T) %>%
  as.data.frame()

# Prepare data for matching
# substring extraction derived from https://rpubs.com/iPhuoc/stringr_manipulation
# stringr and regex help from https://stringr.tidyverse.org/articles/regular-expressions.html
# Eliminate common words in library names like "the" "library" etc.
erate_libs_for_matching <-
  erate_libs_for_matching %>%
  mutate(ros_longitude = as.numeric(ros_longitude),
         ros_latitude = as.numeric(ros_latitude)) %>%
  mutate(
    ros_entity_number = as.numeric(ros_entity_number),
    ros_physical_state = str_to_lower(str_trim(ros_physical_state, side = "both")),
    ros_physical_city = str_to_lower(str_trim(ros_physical_city, side = "both")),
    organization_name = str_to_lower(str_trim(organization_name, side = "both")),
    org_city = str_to_lower(str_trim(org_city, side = "both")),
    org_state = str_to_lower(str_trim(org_state, side = "both")),
    ros_entity_name_processed = str_replace_all(
      ros_entity_name,
      c(
        "the " = "",
        "library" = "",
        "libraries" = "",
        "branch" = "",
        "public" = "",
        "community" = "",
        "\\bbr\\b" = "",
        "\\blib\\b" = ""
      )
    ),
    ros_entity_name_processed = janitor::make_clean_names(ros_entity_name_processed, case =
                                                            "upper_camel"),
    erate_substring = stringr::str_sub(ros_entity_name_processed, 1, 15)
  )

imls <- imls %>%
  mutate(
    LIBNAME = str_to_lower(stringi::stri_enc_toutf8(LIBNAME)),
    STABR = str_to_lower(str_trim(STABR, side = "both")),
    CITY = str_to_lower(stringi::stri_enc_toutf8(CITY)),
    LIBNAME_PROCESSED = str_replace_all(
      LIBNAME,
      c(
        "the " = "",
        "library" = "",
        "libraries" = "",
        "branch" = "",
        "public" = "",
        "community" = "",
        "\\bbr\\b" = "",
        "\\blib\\b" = ""
      )
    ),
    LIBNAME_PROCESSED = janitor::make_clean_names(LIBNAME_PROCESSED, case =
                                                    "upper_camel"),
    SUBSTRING = stringr::str_sub(LIBNAME_PROCESSED, 1, 15)
  )

# Get the list of states that exist in both erate and imls datasets
states_intersect <-
  str_sort(intersect(erate_libs_for_matching[!is.na(erate_libs_for_matching$ros_latitude) &
                                               !is.na(erate_libs_for_matching$ros_longitude), "ros_physical_state"],
                     imls[!is.na(imls$LATITUDE) &
                            !is.na(imls$LONGITUD), "STABR"]))

# create empty list
geo_list = list()

# geo joining on lat/lon between erate data and outlets data
for (i in 1:length(states_intersect)) {
  geo_list[[i]] <- geo_join(
    erate_libs_for_matching %>%
      filter(ros_physical_state == states_intersect[i],
             !(is.na(ros_latitude) | is.na(ros_longitude)
      )),
    imls %>%
      filter(STABR == states_intersect[i],
             !(is.na(LATITUDE) | is.na(LONGITUD))),
    by = c("ros_longitude" = "LONGITUD", "ros_latitude" = "LATITUDE"),
    method = "haversine",
    mode = "inner",
    max_dist = 0.4,
    distance_col = "miles_apart"
  )
}

geo_matches_imls <- bind_rows(geo_list)

# We now have multiple FSCSKEY and FSCS_SEQ columns that we will coalesce later

# What exists in USAC but NOT IMLS?
geo_list = list()

for (i in 1:length(states_intersect)) {
  geo_list[[i]] <- geo_anti_join(
    erate_libs_for_matching %>%
      filter(ros_physical_state == states_intersect[i],!(
        is.na(ros_latitude) | is.na(ros_longitude)
      )),
    imls %>%
      filter(STABR == states_intersect[i],!(is.na(LATITUDE) |
                                           is.na(LONGITUD))),
    by = c("ros_longitude" = "LONGITUD", "ros_latitude" = "LATITUDE"),
    method = "haversine",
    max_dist = 0.4
  )
}

geo_non_matches_erate_imls <- bind_rows(geo_list)

# Now we can eliminate the .x and rename the .y
geo_matches_imls <- geo_matches_imls %>%
  select(-FSCSKEY.x, -FSCS_SEQ.x) %>%
  rename(FSCSKEY = FSCSKEY.y,
         FSCS_SEQ = FSCS_SEQ.y) %>%
  relocate(c(FSCSKEY, FSCS_SEQ), .after = ros_entity_number) # relocate FSCSKEY and FSCS_SEQ column

# The geo_matches_imls dataset contains many duplicate libraries because multiple libraries from IMLS matched the distance specifications,
# thus duplicating libraries in the USAC data. We need to choose the best of the multiple matches. We'll create a custom algorithm for this
# built by trial and error.
geo_string_match <- geo_matches_imls %>%
  mutate(
    LIBNAME = iconv(LIBNAME, "UTF-8", "UTF-8", sub = ''),
    ADDRESS = iconv(ADDRESS, "UTF-8", "UTF-8", sub = ''),
    ros_physical_address = str_to_lower(str_trim(ros_physical_address, side = "both")),
    ADDRESS = str_to_lower(str_trim(ADDRESS, side = "both")),
    # Add string distance calculations
    sub_dist = stringdist::stringdist(erate_substring, SUBSTRING, method = "jw"),
    name_dist = stringdist::stringdist(ros_entity_name, LIBNAME, method = "jw"),
    add_dist = stringdist::stringdist(ros_physical_address, ADDRESS, method = "jw")
  ) %>%
  rowwise() %>%
  mutate(sum_distances = sum(sub_dist, name_dist, add_dist, na.rm = T)) %>%
  group_by(ros_entity_number) %>%
  arrange(sum_distances) %>%
  # The next line keeps the rows where sum_distances is na see https://stackoverflow.com/a/44015049/5593458
  slice(unique(c(which.min(sum_distances)))) %>%
  filter(sum_distances < 1.2 | add_dist < 0.1) %>%
  # Add back in the already matched entities
  full_join(
    matches,
    by = c(
      "ros_entity_number",
      "ros_physical_state",
      "FSCSKEY",
      "FSCS_SEQ"
    )
  )

# What didn't end up matching?
rosnomatch <- base::setdiff(unique(erate_libs$ros_entity_number),
                            geo_string_match$ros_entity_number)

# Make the list into a dataframe
non_matches <-
  data.frame(
    ros_entity_number = matrix(
      unlist(rosnomatch),
      nrow = length(rosnomatch),
      byrow = T
    ),
    stringsAsFactors = FALSE
  )

# Add variables back in to the non-matched recipient numbers
non_matches <- non_matches %>%
  mutate(ros_entity_number = as.numeric(ros_entity_number)) %>%
  # Add in additional information as the dataset is currently just ros_entity_numbers
  left_join(
    erate_libs %>%
      mutate(ros_entity_number = as.numeric(ros_entity_number)) %>%
      select(
        ros_entity_number,
        ros_entity_name,
        ros_physical_zipcode,
        ros_entity_type,
        ros_subtype,
        ros_physical_address,
        ros_physical_city,
        ros_physical_state
      ),
    by = "ros_entity_number"
  ) %>%
  distinct(ros_entity_number, .keep_all = T)

# Create a new df to use that starts with the non-matches, adds in processed lib names
# from erate_libs_for_matching, cleans strings
fuzzy_string_test <- non_matches %>%
  mutate(ros_entity_number = as.numeric(ros_entity_number)) %>%
  left_join(
    erate_libs_for_matching %>%
      select(
        ros_entity_number,
        ros_entity_name
      ),
    by = c("ros_entity_number", "ros_entity_name")
  ) %>%
  mutate(
    ros_physical_state = str_to_lower(str_trim(ros_physical_state, side = "both")),
    ros_physical_city = str_to_lower(str_trim(ros_physical_city, side = "both")),
    ros_entity_name_processed = str_replace_all(
      ros_entity_name,
      c(
        "the " = "",
        "library" = "",
        "libraries" = "",
        "branch" = "",
        "public" = "",
        "community" = "",
        "\\bbr\\b" = "",
        "\\blib\\b" = ""
      )
    ),
    ros_entity_name_processed = janitor::make_clean_names(ros_entity_name_processed, case =
                                                            "upper_camel"),
    erate_substring = stringr::str_sub(ros_entity_name_processed, 1, 15)
  )

# Get the list of zip codes that exist in both datasets
imlszip <-
  intersect(fuzzy_string_test$ros_physical_zipcode,
            imls$ZIP)

# string matching between erate data and outlets data
name_list = list()

# match on substrings by zipcode
for (i in 1:length(imlszip)) {
  name_list[[i]] <- fuzzy_string_test %>%
    filter(ros_physical_zipcode == imlszip[i]) %>%
    stringdist_join(
      imls %>%
        filter(ZIP == imlszip[i]),
      by = c("ros_entity_name_processed" = "LIBNAME_PROCESSED"),
      max_dist = 0.3,
      mode = "left",
      method = "jw",
      distance_col = "substring_dist"
    )
}

name_matches_zip <- bind_rows(name_list)

name_matches_zip <- name_matches_zip %>%
  group_by(ros_entity_number) %>%
  slice_min(substring_dist) %>%
  distinct(ros_entity_number, FSCSKEY, FSCS_SEQ, .keep_all = T)

# Add name_matches_zip to geo_string_match dataset if there are more than 0 matches
if (nrow(name_matches_zip > 0)) {
  geo_string_match <- dplyr::union(
    geo_string_match %>%
      select(
        ros_entity_number,
        ros_physical_state,
        FSCSKEY,
        FSCS_SEQ
      ),
    name_matches_zip %>%
      select(
        ros_entity_number,
        ros_physical_state,
        FSCSKEY,
        FSCS_SEQ
      )
  )
} else {
  geo_string_match <- geo_string_match %>%
    select(
      ros_entity_number,
      ros_physical_state,
      FSCSKEY,
      FSCS_SEQ
    )
}

# Add in ros_entity_name and ros_physical_address
geo_string_match <- geo_string_match %>% 
  left_join(
    erate_libs %>% 
      group_by(ros_entity_number) %>% 
      slice_max(funding_year, with_ties = FALSE) %>% 
      select(ros_entity_number, ros_entity_name, ros_physical_address),
    by = "ros_entity_number"
  )

# Add in the column showing the most recent PLS instance of the entity
geo_string_match <- geo_string_match %>% 
  left_join(imls %>% 
              group_by(FSCSKEY, FSCS_SEQ) %>% 
              slice_max(PLS_YEAR, with_ties = FALSE) %>% 
              select(STABR, FSCSKEY, FSCS_SEQ, LIBNAME, ADDRESS, PLS_YEAR) %>% 
              rename(MOST_RECENT_PLS = PLS_YEAR),
            by = c("FSCSKEY", "FSCS_SEQ")) %>% 
  distinct()

# Write to s3 bucket
s3write_using(geo_string_match,
              FUN = write.csv,
              bucket = "erate-data/data/USAC_IMLS_Match",
              object = "USAC_IMLS_MATCHED.csv")

print("Rosetta stone written to s3")

tmp <-
  base::setdiff(non_matches$ros_entity_number,
                name_matches_zip$ros_entity_number)

# Create a df out of the list called tmp
non_matches_2 <-
  data.frame(
    ros_entity_number = matrix(unlist(tmp), nrow = length(tmp), byrow = T),
    stringsAsFactors = FALSE
  )

# Add back in other variables
non_matches_2 <- non_matches_2 %>%
  left_join(non_matches,
            by = "ros_entity_number")

# Write to s3 bucket
s3write_using(non_matches_2,
              FUN = write.csv,
              bucket = "erate-data/data/USAC_IMLS_Match",
              object = "USAC_Libs_Not_Matched_to_IMLS.csv")

print("Non matches written to s3")

print(Sys.time())

rm(list = ls())
