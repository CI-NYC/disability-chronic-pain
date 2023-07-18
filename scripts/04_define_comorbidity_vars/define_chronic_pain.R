################################################################################
################################################################################
###  CREATE CHRONIC PAIN VARIABLES
###  Kat Hoffman, March 2023
###  Purpose: clean TAFOTH and TAFIPH files for chronic pain ICD codes
###  Output: cleaned data file containing minimum date the beneficiary ("data/final/chronic_pain.rds")
###        has a chronic pain ICD code in the study duration
###         and indicators of whether it occurs in washout or overall study duration
################################################################################
################################################################################

# Set up -----------------------------------------------------------------------

# load libraries
library(arrow)
library(tidyverse)
library(lubridate)
library(data.table)
library(tictoc)
library(foreach)
library(future)
library(furrr)
library(ggalluvial)
library(doParallel)
# options(cores=50)
registerDoParallel()
plan(multicore)
getDoParWorkers()

# Readin OTH and IPH as arrow datsets -----------------------------------------------------------------------

td <- "/home/data/12201/" # directory of interest
oth_files <- paste0(list.files(td, pattern = "*TAFOTH*", recursive = TRUE)) # files of interest
oth <- open_dataset(paste0(td, oth_files), format="parquet") # arrow dataset

iph_files <- paste0(list.files(td, pattern = "*TAFIPH*", recursive = TRUE)) # files of interest
iph <- open_dataset(paste0(td, iph_files), format="parquet") # arrow dataset

# read in cohort dates file
dts_cohorts <- open_dataset("data/tafdedts/dts_cohorts.parquet") |>
    collect() |> 
    mutate(index = rep(1:17, length.out=n())) # split into 17 files (1 million rows each)

# read in all icd chronic pain codes
chronic_pain_icds <- read_csv("input/ICD_codes/chronic_pain_icd10_20230216.csv") |>
    filter(CRITERIA == "Inclusion")

############################################################################
############################################################################
# Step 1: read in all pain codes created in  define_pain.R script
############################################################################
############################################################################


pain_all <- read_rds("data/tmp/pain_all.rds")

# add in pain categories
pain_all_adj <- 
    pain_all |>
    left_join(chronic_pain_icds |> select(pain_cat = PAIN_CAT,
                                                   dgcd = ICD9_OR_10)) |>
    select(BENE_ID, pain_cat, dgcd_dt) |>
    distinct()

# pain_cat                                                n
# <chr>                                               <int>
# 1 Arthritis/Joint/Bone Pain (Other than Back/Neck) 42561532
# 2 Back Pain                                        21991078
# 3 Back/Neck Pain Unspecified                         146214
# 4 Headache                                          3021709
# 5 Misc Pain                                         8498759
# 6 Neck Pain                                         8088842
# 7 Neurologic Pain                                   7368100

#### CREATE FILTER DATA FRAMES (too large to use with future)

headaches_df <-
    pain_all_adj |>
    filter(pain_cat == "Headache") |>
    left_join(dts_cohorts |> select(BENE_ID, washout_start_dt)) 

arthritis_df <-
    pain_all_adj |>
    filter(pain_cat == "Arthritis/Joint/Bone Pain (Other than Back/Neck)") |>
    left_join(dts_cohorts |> select(BENE_ID, washout_start_dt)) 

back_df <-
    pain_all_adj |>
    filter(pain_cat == "Back Pain") |>
    left_join(dts_cohorts |> select(BENE_ID, washout_start_dt)) 

backneck_unsp_df <-
    pain_all_adj |>
    filter(pain_cat == "Back/Neck Pain Unspecified") |>
    left_join(dts_cohorts |> select(BENE_ID, washout_start_dt)) 

misc_df <-
    pain_all_adj |>
    filter(pain_cat == "Misc Pain") |>
    left_join(dts_cohorts |> select(BENE_ID, washout_start_dt)) 

neck_df <-
    pain_all_adj |>
    filter(pain_cat == "Neck Pain") |>
    left_join(dts_cohorts |> select(BENE_ID, washout_start_dt)) 

neuro_df <-
    pain_all_adj |>
    filter(pain_cat == "Neurologic Pain") |>
    left_join(dts_cohorts |> select(BENE_ID, washout_start_dt)) 

### FUNCTION TO MAP OVER PAIN CAT DFS

rolling_windows <- function(pain_cat_df, pain_cat_name, month_start){
    print(paste(month_start, pain_cat_name, Sys.time()))
    
    relevant_pain_dts <-
        pain_cat_df |>
        # keep only codes within the 6 month window of interest
        mutate(start_month = washout_start_dt + months(month_start),
               end_month = washout_start_dt + months(month_start) + months(6)) |>
        # then, filter the diagnosis codes to only contain those filled within the relevant time frame
        filter(dgcd_dt %within% interval(start_month, end_month)) |>
        group_by(BENE_ID, pain_cat) |>
        add_count() |> # add number of dg codes within this window per beneficiary
        filter(n > 1)  |> # only keep codes that show up more than once 
        mutate(first_pain = min(dgcd_dt),
               pain_90 = first_pain + days(90)) |>
        filter(!(dgcd_dt %within% interval(first_pain, pain_90))) # filter out first pain and everything within 90 days

    chronic_pain_per_month <-
        relevant_pain_dts |>
        ungroup() |>
        select(BENE_ID) |>
        distinct() |>
        mutate(month = month_start)
    
    chronic_pain_per_month[[pain_cat_name]] <- 1
    
    saveRDS(chronic_pain_per_month, paste0("data/tmp/chronic_pain_pieces/", pain_cat_name, "_month_", month_start, ".rds"))
    
    print(paste(month_start, pain_cat_name, "COMPLETE", Sys.time()))
    
    return(chronic_pain_per_month)
}

options(future.globals.maxSize= 3000000000)
future_map(0:17, ~rolling_windows(headaches_df, "headache",  .x))
future_map(0:17, ~rolling_windows(backneck_unsp_df, "backneck_unsp",  .x))
future_map(0:17, ~rolling_windows(misc_df, "misc",  .x))
future_map(0:17, ~rolling_windows(neuro_df, "neuro",  .x))
future_map(0:17, ~rolling_windows(neck_df, "neck",  .x))
future_map(0:17, ~rolling_windows(arthritis_df, "arthritis",  .x))
future_map(0:17, ~rolling_windows(back_df, "back",  .x))

dir <- "data/tmp/chronic_pain_pieces/"
files_all <- list.files(dir)

overall_pain_by_month <- function(month_number){
    month_files <- files_all[which(str_detect(files_all, paste0("month_", month_number, ".rds")))]
    month_df <- map(month_files, ~read_rds(paste0(dir, .x))) |>
        reduce(full_join)  |>
        mutate(across(where(is.numeric), ~replace_na(.x, 0))) |>
        mutate(chronic_pain_n = arthritis + back + backneck_unsp + headache + misc + neck + neuro,
               chronic_pain_any = 1) |> # only in this data set if they have chronic pain for that month
        select(BENE_ID, month, chronic_pain_n, chronic_pain_any)
    return(month_df)
}

chronic_pain_all_months <- map_dfr(0:17, overall_pain_by_month)
saveRDS(chronic_pain_all_months, "data/tmp/chronic_pain_all_months.rds")

chronic_pain_all_months <- read_rds("data/tmp/chronic_pain_all_months.rds")

chronic_pain_wide <- pivot_wider(chronic_pain_all_months,
                                 id_cols = BENE_ID,
                                 names_from = month,
                                 names_prefix = "chronic_pain_any_month_",
                                 values_from = chronic_pain_any,
                                 values_fill = 0) |>
    mutate(chronic_pain_n_months = rowSums(across(where(is.numeric)))) 

saveRDS(chronic_pain_wide, "data/final/chronic_pain_wide.rds")
