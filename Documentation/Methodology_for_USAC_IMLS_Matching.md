# Methodology
## Matching USAC Erate Libraries with IMLS PLS Libraries

Preparation steps (order of operations is not important):

- Import the most up-to-date dataset of libraries receiving Erate commitments. We'll refer to this as the **erate_libraries** dataset.
- Import the dataset created previously of library matches between USAC Erate and the IMLS PLS. We'll refer to this as the **matches** dataset.
- Import the dataset of all unique entities found in the IMLS PLS since 2014. This was created after creatively merging the outlets and administrative entities by year. We'll refer to this as the **imls_unique_entities** dataset.
- **If** the hand_matches .csv file has been updated, import that dataset. We'll refer to this as the **hand_matches** dataset.

Methodology (order of operations is of utmost importance):

1. **If** the *hand_matches* dataset was imported, merge the data with the *matches* dataset. We will refer to this merged dataset as the **matches** dataset throughout the rest of this document.
2. Using the *matches* dataset, merge the FSCSKEY and FSCS_SEQ fields into the *erate_libraries* dataset. You will be left with rows in which the FSCSKEY and FSCS_SEQ fields are blank/null. This will be an indicator that we have not already made a match for that library with the IMLS PLS data.
3. Create a new dataset from *erate_libraries* that only includes those where where the FSCSKEY and FSCS_SEQ fields are blank/null. We'll refer to this as the **erate_libs_for_matching** dataset. 
4. Filter out the following ros_entity_number's from *erate_libs_for_matching* as we know that they will never match with an IMLS PLS library (and we don't want the algorithm to try making a match):
   1. 17012400
   2. 137653
   3. 138097
   4. 137724
   5. 231108
   6. 16030444
   7. 126021
   8. 17011387
   9. 16062292
   10. 133460
   11. 17009767
5.  Keep only distinct entities within the *erate_libs_for_matching*. Distinct-ness can be determnined by ros_entity_number and ros_physical_state. Keep the most recent row (based on funding_year) as we will assume that has the most up-to-date name and physical location of each library.