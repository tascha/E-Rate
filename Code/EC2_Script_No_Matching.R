# This is the script that is run on a schedule on an AWS EC2 virtual machine.
# The script wrangles the data that is used in our erate analysis and erate dashboards
# Comments within the script describe the data wrangling steps

# Print the time just to have a record of when the script starts
# A time print happens at the end of the script also
print(Sys.time())

# Load R libraries used within the script
library(RSocrata)
library(dplyr)
library(tidyr)
library(stringr)
library(glue)
library(purrr)
library(janitor)
library(rqdatatable)
library(aws.s3)
library(aws.ec2metadata)

# Gather the c2 budget dataset
# The C2 Budget Tool Data FY2021+ Set is designed to assist applicants in determining their C2 budgets for 
# each five-year cycle beginning with the first budget cycle, which starts in FY2021 and ends in FY2025. 
# filter the API call to applicant_types that include the word Library
c2budget <- read.socrata(
  "https://opendata.usac.org/resource/6brt-5pbv.json?$where=starts_with(applicant_type,%20'Library')",
  app_token = Sys.getenv("USAC_Socrata")
)

# Write to s3 bucket
s3write_using(c2budget,
              FUN = write.csv,
              row.names = F,
              bucket = "erate-data/data/6BRT-5PBV_Cat2Budgets",
              object = "Category_2_Budgets_2021-2025_6BRT-5PBV.csv")

print("c2budget dataset written to s3")

# Remove the dataset from memory
rm(c2budget)

# Gather the full list of years available in the dataset and create a list
# This done with a select distinct query from the API
commitment_years <-
  read.csv(
    "https://opendata.usac.org/resource/avi8-svp9.csv?$select=distinct%20funding_year"
  )[[1]]

# Gather category 1 commitment data for funded library recipients

# Create two empty lists
filteredLibsCat1 = list()
allLibsCat1 = list()

# Loop through each funding year 
# Gather commitments data filtering by the year in the loop, 
# chosen_category_of_service equal to Category 1, 
# form_471_status_name equal to Committed,
# form_471_frn_status_name equal to Funded
for (i in 1:length(commitment_years)) {
  # Loop through each year gathering the data from the API
  filteredLibsCat1[[i]] <- read.socrata(
    glue(
      "https://opendata.usac.org/resource/avi8-svp9.json?chosen_category_of_service=Category1&funding_year={commitment_years[i]}&form_471_status_name=Committed&form_471_frn_status_name=Funded"
    ),
    # Parameters: Category 1, Funding Year, Committed
    app_token = Sys.getenv("USAC_Socrata")
  )
  
  # if no data was gathered (the length of the dataset is 0) just print empty and move to the next year
  if (dim(filteredLibsCat1[[i]])[2] == 0) {
    print("empty")
  }
  
  # if data exists in the gathered dataset, then start filtering
  else{
    # the allLibsCat1 list will contain ALL the data for Library, Library System, and Consortium
    # this is used later in the script in conjunction with disbursement data
    allLibsCat1[[i]] <- filteredLibsCat1[[i]] %>% 
      # Note that this org_entity_type_name filter SHOULD NOT be done in the API call 
      filter(organization_entity_type_name %in% c("Library", "Library System", "Consortium"))
    
    # the filteredLibsCat1 list will contain the filtered and calculated data
    filteredLibsCat1[[i]] <- filteredLibsCat1[[i]] %>%
      # Add a column indicating how many rows exist for a unique combo of billed entity no and 471 line item no
      # in other words how many recipients of the commitment will there be
      # call this column count_ros
      add_count(billed_entity_number, form_471_line_item_number, name = "count_ros") %>%
      # Add estimated amount received by individual recipients by dividing 
      # post_discount_extended_eligible_line_item_costs by the count_ros 
      # call this column cat1_discount_by_ros_estimated
      mutate(cat1_discount_by_ros_estimated = as.numeric(post_discount_extended_eligible_line_item_costs) /
               count_ros) %>%
      # clean text in certain columns - make lowercase and remove trailing spaces
      mutate(
        organization_entity_type_name = str_to_lower(str_trim(organization_entity_type_name, side = "both")),
        ros_entity_type = str_to_lower(str_trim(ros_entity_type, side = "both")),
        ros_entity_name = str_to_lower(str_trim(ros_entity_name, side = "both")),
        ros_subtype = str_to_lower(str_trim(ros_subtype, side = "both"))
      ) %>%
      # Keep only ros_entity_type libraries or NIFs
      filter(
        stringr::str_detect(ros_entity_type, "libr") |
          ros_entity_type == "non-instructional facility (nif)"
      ) %>%
      # Keep ros_entity_type that contain 'libr' OR
      # NIFs that have libr in the org_entity_type_name OR
      # NIFs that are part of consortia and have library in the name
      filter(
        str_detect(ros_entity_type, "libr") |
          (
            ros_entity_type == "non-instructional facility (nif)" &
              str_detect(organization_entity_type_name, "libr")
          ) |
          (
            ros_entity_type == "non-instructional facility (nif)" &
              organization_entity_type_name == "consortium" &
              str_detect(ros_entity_name, "libr")
          ),
        # Keep ros_subtypes that are null or NOT public schools
        (
          is.na(ros_subtype) | !str_detect(ros_subtype, "public school")
        )
      )
    # Write to s3 bucket
    s3write_using(filteredLibsCat1[[i]],
                  FUN = write.csv,
                  row.names = F,
                  bucket = "erate-data/data/AVI8-SVP9_Commitments",
                  object = glue("{commitment_years[i]}_Libraries_Funded_Committed_Category_1.csv")
                  )
    print(glue("{commitment_years[i]} Cat 1 Libraries Funded Committed Written to S3"))    
  }
}

# Bind the yearly dataframes into larger dataframes
cat1_libs_consortia <- bind_rows(allLibsCat1)
cat1_filtered_libs <- bind_rows(filteredLibsCat1)

# Gather category 2 commitment data for funded library recipients

# Create two empty lists
filteredLibsCat2 = list()
allLibsCat2 = list()

# Loop through each funding year 
# Gather commitments data filtering by the year in the loop, 
# chosen_category_of_service equal to Category 2, 
# form_471_status_name equal to Committed,
# form_471_frn_status_name equal to Funded
for (i in 1:length(commitment_years)) {
  filteredLibsCat2[[i]] <- read.socrata(
    glue(
      "https://opendata.usac.org/resource/avi8-svp9.json?chosen_category_of_service=Category2&funding_year={commitment_years[i]}&form_471_status_name=Committed&form_471_frn_status_name=Funded"
    ),
    # Parameters: Category 2, Funding Year, Committed, Funded
    app_token = Sys.getenv("USAC_Socrata")
  )
  
  # if no data was gathered (the length of the dataset is 0) just print empty and move to the next year
  if (dim(filteredLibsCat2[[i]])[2] == 0) {
    print("empty")
  }
  
  # if data exists in the gathered dataset, then start filtering
  else{
    # the allLibsCat2 list will contain ALL the data for Library, Library System, and Consortium
    # this is used later in the script in conjunction with disbursement data
    allLibsCat2[[i]] <- filteredLibsCat2[[i]] %>% 
      # Note that this org_entity_type_name filter SHOULD NOT be done in the API call 
      filter(organization_entity_type_name %in% c("Library", "Library System", "Consortium"))
    
    # the filteredLibsCat2 list will contain the filtered and calculated data
    filteredLibsCat2[[i]] <- filteredLibsCat2[[i]] %>%
      # Add a column indicating how many rows exist for a unique combo of billed entity no and 471 line item no
      # in other words how many recipients of the commitment will there be
      # call this column count_ros
      # (this might not be useful for Cat2 but we'll add it to be consistent with the Cat1 data)
      add_count(billed_entity_number, form_471_line_item_number, name = "count_ros") %>%
      # Add estimated amount received by individual recipient by multiplying
      # the discount percent (dis_pct) by original_allocation
      # call this column cat2_discount_by_ros 
      mutate(cat2_discount_by_ros = as.numeric(original_allocation) * as.numeric(dis_pct)) %>%
      # clean text in certain columns - make lowercase and remove trailing spaces
      mutate(
        organization_entity_type_name = str_to_lower(str_trim(organization_entity_type_name, side = "both")),
        ros_entity_type = str_to_lower(str_trim(ros_entity_type, side = "both")),
        ros_entity_name = str_to_lower(str_trim(ros_entity_name, side = "both")),
        ros_subtype = str_to_lower(str_trim(ros_subtype, side = "both"))
      ) %>%
      # Keep only ros_entity_type libraries and NIFs
      filter(
        stringr::str_detect(ros_entity_type, "libr") |
          ros_entity_type == "non-instructional facility (nif)"
      ) %>%
      # Keep ros_entity_type that contain 'libr' OR
      # NIFs that have libr in the org_entity_type_name OR
      # NIFs that are part of consortia and have library in the name
      filter(
        str_detect(ros_entity_type, "libr") |
          (
            ros_entity_type == "non-instructional facility (nif)" &
              str_detect(organization_entity_type_name, "libr")
          ) |
          (
            ros_entity_type == "non-instructional facility (nif)" &
              organization_entity_type_name == "consortium" &
              str_detect(ros_entity_name, "libr")
          ),
        # Keep ros_subtypes that are null or NOT public schools
        (
          is.na(ros_subtype) | !str_detect(ros_subtype, "public school")
        )
      )
    # Write to s3 bucket
    s3write_using(filteredLibsCat2[[i]],
                  FUN = write.csv,
                  row.names = F,
                  bucket = "erate-data/data/AVI8-SVP9_Commitments",
                  object = glue("{commitment_years[i]}_Libraries_Funded_Committed_Category_2.csv")
    )
    print(glue("{commitment_years[i]} Cat 2 Libraries Funded Committed Written to S3"))   
  }
}

# Bind the yearly dataframes into larger dataframes
cat2_libs_consortia <- bind_rows(allLibsCat2)
cat2_filtered_libs <- bind_rows(filteredLibsCat2)

# Combine the cat1 and cat 2 data into a large dataframe
erate_libs <- bind_rows(cat1_filtered_libs, cat2_filtered_libs)

# Write to s3 bucket
s3write_using(erate_libs,
              FUN = write.csv,
              row.names = F,
              bucket = "erate-data/data/AVI8-SVP9_Commitments",
              object = "Libraries_Funded_Committed_AVI8-SVP9.csv")

print("erate_libs dataset written to s3")

# remove some dataframes that are no longer needed to save space
rm(cat1_filtered_libs, cat2_filtered_libs)

# remove some lists that are no longer needed to save space
rm(allLibsCat1, allLibsCat2, filteredLibsCat1, filteredLibsCat2)

# Gather the full list of funding years available in the qdmp-ygft dataset and create a list
# This done with a select distinct query from the API
disbursement_years <-
  read.csv(
    "https://opendata.usac.org/resource/qdmp-ygft.csv?$select=distinct%20funding_year"
  )[[1]]

disb = list()
# Loop through each available funding year and create a dataset of Funded application disbursements
for (i in 1:length(disbursement_years)) {
  disb[[i]] <- read.socrata(
    glue(
      "https://opendata.usac.org/resource/qdmp-ygft.json?form_471_frn_status_name=Funded&funding_year={disbursement_years[i]}"
    ),
    # Parameters: Status equals Funded, year equals year in loop
    app_token = Sys.getenv("USAC_Socrata")
  )
  
  # Write each year's data to s3 bucket
  s3write_using(disb[[i]],
                FUN = write.csv,
                row.names = F,
                bucket = "erate-data/data/QDMP-YGFT_Disbursements",
                object = glue("{disbursement_years[i]}_Funded_Disbursements.csv"))
  
  print(glue("{disbursement_years[i]}_Funded_Disbursements written to S3"))
}

# Combine each of the years into one big dataset
full_disbursements <- bind_rows(disb)

# Remove the list of dataframes called disb
rm(disb)

# Write the big Funded_Disbursement dataset to s3
s3write_using(full_disbursements,
              FUN = write.csv,
              row.names = F,
              bucket = "erate-data/data/QDMP-YGFT_Disbursements",
              object = "Full_Funded_Disbursements.csv")

print("Full_Funded_Disbursements written to s3")

# create a dataframe of the full libraries and consortia datasets (not filtered down to recipients)
disbursement_merge <- bind_rows(cat1_libs_consortia, cat2_libs_consortia)

# Create a dataframe of funding request numbers that shows how much was requested, how much was disbursed,
# and the percentage of the request that was disbursed
disbursement_stats <- disbursement_merge %>%
  distinct(funding_request_number,
           form_471_line_item_number,
           .keep_all = T) %>%
  mutate(
    post_discount_extended_eligible_line_item_costs =
      as.numeric(post_discount_extended_eligible_line_item_costs),
    funding_request_number = as.numeric(funding_request_number)
  ) %>%
  group_by(funding_request_number) %>%
  summarise(
    post_discount_extended_eligible_line_item_costs_sum =
      sum(post_discount_extended_eligible_line_item_costs)
  ) %>%
  left_join(
    full_disbursements %>%
      select(
        funding_request_number,
        funding_commitment_request,
        total_authorized_disbursement
      ) %>%
      mutate(funding_request_number = as.numeric(funding_request_number)),
    by = "funding_request_number"
  ) %>%
  mutate(
    total_authorized_disbursement = as.numeric(total_authorized_disbursement),
    post_discount_extended_eligible_line_item_costs_sum = as.numeric(post_discount_extended_eligible_line_item_costs_sum),
    pct_of_committed_received = total_authorized_disbursement / post_discount_extended_eligible_line_item_costs_sum
  )

# Write to s3 bucket
s3write_using(
  disbursement_stats,
  FUN = write.csv,
  row.names = F,
  bucket = "erate-data/data/USAC_Tables_Merged",
  object = "Commitments_AVI8-SVP9_and_Disbursements_QDMP-YGFT.csv"
)

print("disbursement_stats dataset written to s3")

rm(disbursement_merge, disbursement, disbursement_stats)

# Read in IMLS PLS datasets stored in S3
imls_2014to2020 <-
  s3read_using(FUN = read.csv,
               object = "2014-2020_OUTLETs_and_AEs_Merged.csv",
               bucket = "erate-data/data/IMLS_PLS")

# Import matches which were done in a separate script run manually
matches <-
  s3read_using(
    FUN = read.csv,
    na.strings = c("", " ", "N/A", "n/a"),
    object = "USAC_IMLS_MATCHED.csv",
    bucket = "erate-data/data/USAC_IMLS_Match"
  )

# Change REN to numeric data type
matches <- matches %>% 
  mutate(ros_entity_number = as.numeric(ros_entity_number))

# Add FSCSKEY and FSCS_SEQ columns to erate_libs dataset and include the matches
erate_libs <- erate_libs %>%
  mutate(ros_entity_number = as.numeric(ros_entity_number)) %>%
  left_join(matches %>% 
              select(ros_entity_number, FSCSKEY, FSCS_SEQ, MOST_RECENT_PLS),
            by = "ros_entity_number") %>%
  relocate(c("FSCSKEY", "FSCS_SEQ"), .after = ros_entity_number)

# Write to s3 bucket
s3write_using(erate_libs,
              FUN = write.csv,
              row.names = F,
              bucket = "erate-data/data/AVI8-SVP9_Commitments",
              object = "Libraries_Funded_Committed_AVI8-SVP9_with_FSCS_Matches.csv")

print("erate_libs with FSCS dataset written to s3")

# Merge USAC and IMLS
# Left join so that we only keep ERate entities
erate_pls <- erate_libs %>%
  mutate(FSCS_SEQ = as.numeric(FSCS_SEQ),
         funding_year = as.numeric(funding_year)) %>%
  # this join adds matching IMLS outlets records
  left_join(imls_2014to2020 %>% 
              mutate(FSCS_SEQ = as.numeric(FSCS_SEQ),
                     PLS_YEAR = as.numeric(PLS_YEAR)),
            by = c('FSCSKEY' = 'FSCSKEY', 
                   'FSCS_SEQ' = 'FSCS_SEQ', 
                   'funding_year' = 'PLS_YEAR')) 

# This adds in some fields for funding years 2021-2022 for which we don't have a PLS yet
temp_imls <- erate_pls %>% 
  mutate(FSCS_SEQ = as.numeric(FSCS_SEQ)) %>% 
  filter(is.na(LIBNAME)) %>% 
  rquery::natural_join(imls_2014to2020 %>% 
                         mutate(FSCS_SEQ = as.numeric(FSCS_SEQ)) %>% 
                         group_by(FSCSKEY, FSCS_SEQ) %>% 
                         slice_max(PLS_YEAR, with_ties = FALSE) %>%
                         select(FSCSKEY, FSCS_SEQ, STABR, LIBNAME, LIBID, ADDRESS, CITY, ZIP, CNTY, SQ_FEET, LOCALE, LOCALE_ADD),
                       by = c("FSCSKEY", "FSCS_SEQ"),
                       jointype = "LEFT")

# Add in a column with the descriptor fields for LOCAL_ADD
# These descriptors are found in the documentation for the IMLS PLS dataset
erate_pls <- erate_pls %>% 
  filter(!is.na(LIBNAME)) %>%
  bind_rows(temp_imls)%>% 
  mutate(
    LOCALE_ADD_DESCR = case_when(
      LOCALE_ADD == 11 ~ "City Large",
      LOCALE_ADD == 12 ~ "City Midsize",
      LOCALE_ADD == 13 ~ "City Small",
      LOCALE_ADD == 21 ~ "Suburban Large",
      LOCALE_ADD == 22 ~ "Suburban Midsize",
      LOCALE_ADD == 23 ~ "Suburban Small",
      LOCALE_ADD == 31 ~ "Town Fringe",
      LOCALE_ADD == 32 ~ "Town Distant",
      LOCALE_ADD == 33 ~ "Town Remote",
      LOCALE_ADD == 41 ~ "Rural Fringe",
      LOCALE_ADD == 42 ~ "Rural Distant",
      LOCALE_ADD == 43 ~ "Rural Remote",
      LOCALE == 11 ~ "City Large",
      LOCALE == 12 ~ "City Midsize",
      LOCALE == 13 ~ "City Small",
      LOCALE == 21 ~ "Suburban Large",
      LOCALE == 22 ~ "Suburban Midsize",
      LOCALE == 23 ~ "Suburban Small",
      LOCALE == 31 ~ "Town Fringe",
      LOCALE == 32 ~ "Town Distant",
      LOCALE == 33 ~ "Town Remote",
      LOCALE == 41 ~ "Rural Fringe",
      LOCALE == 42 ~ "Rural Distant",
      LOCALE == 43 ~ "Rural Remote"
    )
  )
  

# Write to s3 bucket
s3write_using(erate_pls,
              FUN = write.csv,
              row.names = F,
              bucket = "erate-data/data/AVI8-SVP9_Commitments",
              object = "Libraries_Funded_Committed_AVI8-SVP9_with_PLS_Data.csv")

print("erate_imls written to s3")


# Make a smaller dataset for use on Shiny dashboard
erate_imls_compact <- erate_pls %>%
  select(
    ros_entity_number,
    FSCSKEY,
    FSCS_SEQ,
    ros_entity_name,
    ros_entity_type,
    ros_physical_city,
    ros_physical_state,
    ros_congressional_district,
    ros_urban_rural_status,
    organization_entity_type_name,
    funding_year,
    chosen_category_of_service,
    form_471_service_type_name,
    ros_square_footage,
    original_allocation,
    cat1_discount_by_ros_estimated,
    cat2_discount_by_ros,
    ros_latitude,
    ros_longitude,
    LATITUDE,
    LONGITUD,
    STABR,
    LIBNAME,
    LOCALE,
    LOCALE_ADD,
    LOCALE_ADD_DESCR
  )

# Write to s3 bucket
s3write_using(
  erate_imls_compact,
  FUN = write.csv,
  row.names = F,
  bucket = "erate-data/data/AVI8-SVP9_Commitments",
  object = "erate_imls_compact.csv"
)

print("erate_imls_compact written to s3")

print(Sys.time())

rm(list = ls())

