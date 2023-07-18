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
chronic_pain_icds <- read_csv("input/ICD_codes/chronic_pain_icd10_20230216.csv")  |>
    filter(CRITERIA == "Inclusion") 


############################################################################
############################################################################
# Step 1: in parallel across the 17 beneficiary splits, extract OTH codes and 
#       keep only the diagnosis codes (1 and 2, separately) which are in the chronic pain
#       ICD code list, save as a "raw_i.parquet tmp split file
############################################################################
############################################################################

still <- 1:17
still <- c(5, 9, 13, 15, 16) # failed cores

foreach(i = still) %dopar%
       {
           print(paste(i, Sys.time()))
        ids <- dts_cohorts |> filter(index == i) |> pull(BENE_ID)
        dg1 <- 
            oth |> 
            filter(BENE_ID %in% ids) |>
            select(BENE_ID, SRVC_BGN_DT, SRVC_END_DT, DGNS_CD_1) |>
            rename(dgcd = DGNS_CD_1) |>
            filter(dgcd %in% chronic_pain_icds$ICD9_OR_10) |>
            arrange(SRVC_BGN_DT) |>
            collect() 
        print(paste(i, Sys.time()))
        dg2 <- 
            oth |> 
            filter(BENE_ID %in% ids) |>
            select(BENE_ID, SRVC_BGN_DT, SRVC_END_DT, DGNS_CD_2) |>
            rename(dgcd = DGNS_CD_2) |>
            filter(dgcd %in% chronic_pain_icds$ICD9_OR_10) |>
            arrange(SRVC_BGN_DT) |>
            collect()
        all_dg <- bind_rows(dg1, dg2)
        write_parquet(all_dg, paste0("data/tafoth/tmp_splits_pain/raw/", i, ".parquet"))
        
       }


############################################################################
############################################################################
# Step 2: extract chronic pain ICD codes from the Inpatient Hospital files
############################################################################
############################################################################

icd_codes_to_check <-
    iph |>
    select(BENE_ID, SRVC_BGN_DT, SRVC_END_DT, contains("DGNS_CD")) |>
    collect()

iph_dg <-
    icd_codes_to_check |>
    mutate(chronic_pain = +(if_any(starts_with("DGNS_CD"),  ~. %in% chronic_pain_icds$ICD9_OR_10))) |>
    filter(chronic_pain == T) |> # only keep chronic pain codes
    left_join(dts_cohorts |> select(BENE_ID, washout_start_dt)) |> # join washout start date in
    #group_by(BENE_ID) |>
    filter(SRVC_BGN_DT >= washout_start_dt)  # only keep begin dates that occur on or after washout date

iph_dg_long <- iph_dg |>
    pivot_longer(cols = contains("DGNS_CD"), names_to = "dgcd_number", values_to = "dgcd") |>
    filter(dgcd %in% chronic_pain_icds$ICD9_OR_10)

write_parquet(iph_dg_long, "data/tafiph/pain_iph_all.parquet") # save data set



############################################################################
############################################################################
# Step 3: clean up pain codes from OTH, merge with IPH pain codes
############################################################################
############################################################################

td <- "data/tafoth/tmp_splits_pain/raw/"
pain_oth <- open_dataset(td)

pain_oth_clean <- pain_oth |>
    mutate(dgcd_dt = case_when(is.na(SRVC_BGN_DT) ~ SRVC_END_DT, # if missing service begin date, replace with service end
                               T ~ SRVC_BGN_DT)) |>
    select(BENE_ID, dgcd_dt, dgcd) |>
    collect() |>
    distinct() # cut down on observations/joining computational power

pain_iph_all <- read_parquet("data/tafiph/pain_iph_all.parquet")

pain_iph_clean <-
    pain_iph_all |>
    mutate(dgcd_dt = case_when(is.na(SRVC_BGN_DT) ~ SRVC_END_DT,
                                        T ~ SRVC_BGN_DT)) |>
    select(BENE_ID, dgcd_dt, dgcd) |>
    distinct() # cut down on observations/joining computational power

pain_all <-
    full_join(pain_iph_clean, pain_oth_clean) |> # merge together
    distinct()  # only keep distinct dates / diagnosis codes

saveRDS(pain_all, "data/tmp/pain_all.rds") # this contains all pain (in case want to change minimum date for chronic pain, currently 90 days)
              

############################################################################
############################################################################
# Step 4: add indicators for when the minimum date of pain occurred
############################################################################
############################################################################

pain_all <- read_rds("data/tmp/pain_all.rds")

tic()
# get minimum pain date after washout period starts
pain_min <- pain_all |>
    group_by(BENE_ID) |>
    arrange(dgcd_dt) |> 
    filter(row_number() == 1) |>
    select(BENE_ID, pain_dt = dgcd_dt)
toc()

saveRDS(pain_min, "data/tmp/pain_min.rds")

pain_min <- read_rds("data/tmp/pain_min.rds")

tic()
all_pain_clean <- 
    dts_cohorts |>
    select(BENE_ID, washout_start_dt, washout_cal_end_dt, washout_12mos_end_dt, washout_cont_end_dt,
           study_cal_end_dt, study_cont_end_dt
           ) |>
    left_join(pain_min) |>
    mutate(pain_washout_cal = case_when(pain_dt %within% interval(washout_start_dt, washout_cal_end_dt) ~ 1,
                                                TRUE ~ 0),
           pain_washout_12mos_cal = case_when(pain_dt %within% interval(washout_start_dt, washout_12mos_end_dt) ~ 1,
                                        TRUE ~ 0),
           pain_washout_cont = case_when(pain_dt %within% interval(washout_start_dt, washout_cont_end_dt) ~ 1,
                                                TRUE ~ 0),
           pain_study_cal = case_when(pain_dt %within% interval(washout_start_dt, study_cal_end_dt) ~ 1,
                                              TRUE ~ 0),
           pain_study_cont = case_when(pain_dt %within% interval(washout_start_dt, study_cont_end_dt) ~ 1,
                                               TRUE ~ 0)) |>
    select(BENE_ID, pain_dt, 
           pain_washout_cal, pain_washout_12mos_cal,
           pain_washout_cont,
           pain_study_cal, pain_study_cont)
toc()

write_rds(all_pain_clean, "data/final/pain.rds") # save final data file







