################################################################################
################################################################################
###  CREATE DEPRESSION VARIABLES
###  Kat Hoffman, March 2023
###  Purpose: clean TAFOTH and TAFIPH files for depression ICD codes
###  Output: cleaned data file containing minimum date the beneficiary ("data/final/depression.rds")
###        has a depression ICD code in the study duration
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

# read in all icd depression codes
depression_icds <- read_csv("input/ICD_codes/depression_icd10_20230323.csv", col_names = F) |>
                                rename(ICD9_OR_10 = X1)

############################################################################
############################################################################
# Step 1: in parallel across the 17 beneficiary splits, extract OTH codes and 
#       keep only the diagnosis codes (1 and 2, separately) which are in the depression
#       ICD code list, save as a "raw_i.parquet tmp split file
############################################################################
############################################################################


all <- 1:17
td <- "data/tafoth/tmp_splits_depression/" 
files <- paste0(list.files(td, pattern = "*.parquet", recursive = TRUE)) 
done <- unique(parse_number(files))
still <- all[-which(all %in% done)]

foreach(i = still) %dopar%
    {
        print(paste(i, Sys.time()))
        ids <- dts_cohorts |> filter(index == i) |> pull(BENE_ID)
        dg1 <- 
            oth |> 
            filter(BENE_ID %in% ids) |>
            mutate(SRVC_BGN_DT = case_when(is.na(SRVC_BGN_DT) ~ SRVC_END_DT, TRUE ~ SRVC_BGN_DT)) |>
            select(BENE_ID, SRVC_BGN_DT, DGNS_CD_1) |>
            rename(dgcd = DGNS_CD_1) |>
            filter(dgcd %in% depression_icds$ICD9_OR_10) |>
            arrange(SRVC_BGN_DT) |>
            collect() 
        print(paste(i, Sys.time()))
        dg2 <- 
            oth |> 
            filter(BENE_ID %in% ids) |>
            mutate(SRVC_BGN_DT = case_when(is.na(SRVC_BGN_DT) ~ SRVC_END_DT, TRUE ~ SRVC_BGN_DT)) |>
            select(BENE_ID, SRVC_BGN_DT, DGNS_CD_2) |>
            rename(dgcd = DGNS_CD_2) |>
            filter(dgcd %in% depression_icds$ICD9_OR_10) |>
            arrange(SRVC_BGN_DT) |>
            collect()
        all_dg <- bind_rows(dg1, dg2)
        write_parquet(all_dg, paste0("data/tafoth/tmp_splits_depression/raw_", i, ".parquet"))
        
    }

############################################################################
############################################################################
# Step 2: in parallel across the 17 beneficiary splits, extract OTH codes and 
#       that occur after the washout period begins, and only keep the minimum
#       ICD code list, save as a "i.parquet tmp split file
############################################################################
############################################################################


foreach(i = 1:17) %dopar%
    {
        print(paste(i, Sys.time()))
        all_dg <- read_parquet(paste0("data/tafoth/tmp_splits_depression/raw_", i, ".parquet")) |> collect()
        all_dg_clean <-
            all_dg |>
            left_join(dts_cohorts |> select(BENE_ID, washout_start_dt)) |>
            group_by(BENE_ID) |>
            filter(SRVC_BGN_DT >= washout_start_dt) |>
            summarize(min_depression_dt = min(SRVC_BGN_DT)) |>
            ungroup()
        write_parquet(all_dg_clean, paste0("data/tafoth/tmp_splits_depression/", i, ".parquet"))
    }



############################################################################
############################################################################
# Step 3: extract depression ICD codes from the Inpatient Hospital files
############################################################################
############################################################################

icd_codes_to_check <-
    iph |>
    mutate(SRVC_BGN_DT = case_when(is.na(SRVC_BGN_DT) ~ SRVC_END_DT, TRUE ~ SRVC_BGN_DT)) |>
    select(BENE_ID, SRVC_BGN_DT, contains("DGNS_CD")) |>
    collect()

iph_dg <-
    icd_codes_to_check |>
    mutate(depression = +(if_any(starts_with("DGNS_CD"),  ~. %in% depression_icds$ICD9_OR_10))) |>
    filter(depression == T) |> # only keep depression codes
    left_join(dts_cohorts |> select(BENE_ID, washout_start_dt)) |> # join washout start date in
    group_by(BENE_ID) |>
    filter(SRVC_BGN_DT >= washout_start_dt) |>  # only keep begin dates that occur on or after washout date
    summarize(min_depression_dt = min(SRVC_BGN_DT)) # only keep the minimum date

write_parquet(iph_dg, "data/tafiph/depression_iph.parquet") # save data set

############################################################################
############################################################################
# Step 4: in parallel across the 17 OTH splits, left join the IPH file
#   keep only the minimum depression date between OTH and IPH for that beneficiary
#   save as i_clean.parquet in temp folder
############################################################################
############################################################################

td <- "data/tafoth/tmp_splits_depression/" 
files <- paste0(list.files(td, pattern = "*.parquet", recursive = TRUE)) 
files <- files[-which(str_detect(files, "raw_"))]
oth_splits <- open_dataset(paste0(td, files), format="parquet") # arrow dataset

foreach(i = 1:17) %dopar%
    {
        oth_tmp <- open_dataset(paste0("data/tafoth/tmp_splits_depression/", paste0(i,".parquet")), format="parquet") |>
            collect()
        iph_tmp <- open_dataset(paste0("data/tafiph/depression_iph.parquet")) |>
            collect()
        clean <- 
            oth_tmp |>
            left_join(iph_tmp) |>
            left_join(dts_cohorts |> select(BENE_ID, washout_start_dt)) |>
            filter(min_depression_dt >= washout_start_dt) |> # only keep depression dg codes after washout starts
            group_by(BENE_ID) |>
            summarize(depression_dt = min(min_depression_dt)) |>
            distinct() # some dates are duplicated
        write_parquet(clean, paste0("data/tafoth/tmp_splits_depression/", i, "_clean.parquet"))
    }

# all the cleaned files (all minimum dates except beneficiaries that only occur in IPH)
td <- "data/tafoth/tmp_splits_depression/" 
files <- paste0(list.files(td, pattern = "*_clean.parquet", recursive = TRUE)) 
all_depression_oth <-
    open_dataset(paste0(td, files), format="parquet") |>
    collect() 

iph_dg <- read_parquet("data/tafiph/depression_iph.parquet") |> collect()

############################################################################
############################################################################
# Step 5: add in beneficiaries minimum dates that were only in IPH, not OTH
############################################################################
############################################################################

# pull out beneficiaries that we don't already have in OTH
iph_only <-
    iph_dg |>
    filter(!(BENE_ID %in% all_depression_oth$BENE_ID)) |>
    rename(depression_dt = min_depression_dt)

# bind all the rows together (bene_id, depression_dt)
all_depression <-
    bind_rows(all_depression_oth, iph_only) |>
    arrange(depression_dt) |>
    distinct(BENE_ID, .keep_all = T)


############################################################################
############################################################################
# Step 6: add indicators for when the minimum date of depression occurred
############################################################################
############################################################################

all_depression_clean <- 
    dts_cohorts |>
    left_join(all_depression) |>
    mutate(depression_washout_cal = case_when(depression_dt %within% interval(washout_start_dt, washout_cal_end_dt) ~ 1,
                                           TRUE ~ 0),
           depression_washout_12mos_cal = case_when(depression_dt %within% interval(washout_start_dt, washout_12mos_end_dt) ~ 1,
                                              TRUE ~ 0),
           depression_washout_cont = case_when(depression_dt %within% interval(washout_start_dt, washout_cont_end_dt) ~ 1,
                                            TRUE ~ 0),
           depression_study_cal = case_when(depression_dt %within% interval(washout_start_dt, study_cal_end_dt) ~ 1,
                                         TRUE ~ 0),
           depression_study_cont = case_when(depression_dt %within% interval(washout_start_dt, study_cont_end_dt) ~ 1,
                                          TRUE ~ 0)) |>
    select(BENE_ID, depression_dt, 
           depression_washout_cal,  depression_washout_12mos_cal, depression_washout_cont,
           depression_study_cal, depression_study_cont)

write_rds(all_depression_clean, "data/final/depression.rds") # save final data file
