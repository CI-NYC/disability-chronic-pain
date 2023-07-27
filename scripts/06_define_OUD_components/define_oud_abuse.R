################################################################################
################################################################################
###  Define OUD abuse using Cochran's codes
################################################################################
################################################################################

# Set up -----------------------------------------------------------------------

# load libraries
library(arrow)
library(tidyverse)
library(tidylog)
library(lubridate)
library(data.table)
library(tictoc)
library(future)
library(furrr)
library(doParallel)
# options(cores=50)
registerDoParallel()
plan(multicore)
getDoParWorkers()


source("scripts/ICD_codes/OUD.R")

# Load necessary files ---------------------------------------------------------

dts_cohorts <- open_dataset("data/tafdedts/dts_cohorts.parquet") |>
    collect() |> 
    mutate(index = rep(1:32, length.out=n()))

# Read in data files ---------------------------------------------------------

td <- "/home/data/12201/" # directory of interest

# read in IPH (inpatient hospital)
files <- paste0(list.files(td, pattern = "TAFIPH", recursive = TRUE))
iph <- open_dataset(paste0(td, files), format="parquet")

# read in OTH (other services baseline)
files <- paste0(list.files(td, pattern = "TAFOTH", recursive = TRUE))
oth <- open_dataset(paste0(td, files), format="parquet")

# Filter arrow files by cohort ---------------------------------------------------------

all <- 1:32
td <- "data/oud_info/tmp_splits_oud_abuse_dt/" 
files <- paste0(list.files(td, pattern = "*.parquet", recursive = TRUE)) 
done <- unique(parse_number(files))
still <- all[-which(all %in% done)]

# still <- 1:32
foreach(i = still) %dopar%
    {print(paste(i, Sys.time()))
        
        iter <- dts_cohorts |> filter(index == i) |> pull(BENE_ID)
        
        oth_cohort <- # first extract the other services data
            oth |>
            filter(!is.na(DGNS_CD_1)) |>
            mutate(SRVC_BGN_DT = case_when(is.na(SRVC_BGN_DT) ~ SRVC_END_DT, TRUE ~ SRVC_BGN_DT)) |>
            filter(SRVC_BGN_DT >= as.Date("2016-01-01")) |> # save space, keep only DG codes after 2016
            filter(BENE_ID %in% iter) |> 
            select(BENE_ID, SRVC_BGN_DT, contains("DGNS_CD")) |>
            collect()
        
        iph_cohort <- # then extract IPH data
            iph |>
            filter(BENE_ID %in% iter) |> 
            mutate(SRVC_BGN_DT = case_when(is.na(SRVC_BGN_DT) ~ SRVC_END_DT, TRUE ~ SRVC_BGN_DT)) |>
            filter(SRVC_BGN_DT >= as.Date("2016-01-01")) |> # save space, keep only DG codes after 2016
            select(BENE_ID, ADMSN_DT, SRVC_BGN_DT, contains("DGNS_CD")) |>
            collect()
        
        print(paste(i, "subset", Sys.time()))
        
        # Create OUD component date variables from IPH and OTH ---------------------------------------------------------
        
        oth_all_vars <-
            oth_cohort |> # define oud variable of interest via codes
            mutate(oud_abuse_dt = case_when(DGNS_CD_1 %in% cochran_codes_abuse ~ SRVC_BGN_DT,
                                            DGNS_CD_2 %in% cochran_codes_abuse ~ SRVC_BGN_DT)) |>
            drop_na(oud_abuse_dt) |> # drop anyone who doesn't have a date for the oud var of interest
            distinct(BENE_ID, oud_abuse_dt)
        
        iph_all_vars <-
            iph_cohort |> # for IPH, first need to pivot to long format because there are 12 DG codes
            pivot_longer(cols = contains("DGNS_CD"), names_to = "dg_num", values_to = "cd") |> # switch to long format data because there are 12 dx codes and IPH isn't too big
            drop_na(cd) |> # drop any missing icd code (cd) rows
            mutate(oud_abuse_dt = case_when(cd %in% cochran_codes_abuse ~ SRVC_BGN_DT)) |>  # define oud variable of interest via codes
            drop_na(oud_abuse_dt) |> # drop anyone who doesn't have a date for the oud var of interest
            distinct(BENE_ID, oud_abuse_dt)
        
        all <- bind_rows(oth_all_vars, iph_all_vars) # stack the columns (BENE_ID and oud var of interest) on top of each other
        
        write_parquet(all, paste0("data/oud_info/tmp_splits_oud_abuse_dt/", i, ".parquet")) # save the split
        
    }

# Collect all the splits and merge to one data set

td <- "data/oud_info/tmp_splits_oud_abuse_dt/" 
files <- paste0(list.files(td, pattern = "*.parquet", recursive = TRUE)) 
all_oud_abuse_dt <-
    open_dataset(paste0(td, files), format="parquet") |>
    select(BENE_ID, oud_abuse_dt) |>
    collect()

# Keep only the minimum date of the OUD variable of interest that occurs after the washout period starts for that beneficiary ID 
min_oud_abuse_dt <- 
    all_oud_abuse_dt |>
    left_join(dts_cohorts |> select(BENE_ID, washout_start_dt)) |>
    filter(oud_abuse_dt >= washout_start_dt) |> # only keep oud abuse diagnosis codes that occur during washout period
    select(BENE_ID, oud_abuse_dt) |>
    arrange(oud_abuse_dt) |>
    distinct(BENE_ID, .keep_all = T) # only keep first date after washout (already arranged by date)


oud_abuse_clean <- 
    dts_cohorts |>
    left_join(min_oud_abuse_dt) |>
    mutate(oud_abuse_washout_cal = case_when(oud_abuse_dt %within% interval(washout_start_dt, washout_cal_end_dt) ~ 1,
                                                TRUE ~ 0),
           oud_abuse_washout_12mos_cal = case_when(oud_abuse_dt %within% interval(washout_start_dt, washout_12mos_end_dt) ~ 1,
                                             TRUE ~ 0),
           oud_abuse_washout_cont = case_when(oud_abuse_dt %within% interval(washout_start_dt, washout_cont_end_dt) ~ 1,
                                                 TRUE ~ 0),
           oud_abuse_study_cal = case_when(oud_abuse_dt %within% interval(washout_start_dt, study_cal_end_dt) ~ 1,
                                              is.na(study_cal_end_dt) & oud_abuse_dt > washout_start_dt ~ 1,
                                              TRUE ~ 0),
           oud_abuse_study_cont = case_when(oud_abuse_dt %within% interval(washout_start_dt, study_cont_end_dt) ~ 1,
                                               is.na(study_cont_end_dt) & oud_abuse_dt > washout_start_dt ~ 1,
                                               TRUE ~ 0)) |>
    select(BENE_ID, oud_abuse_dt, 
           oud_abuse_washout_cal, oud_abuse_washout_12mos_cal,
           oud_abuse_washout_cont,
           oud_abuse_study_cal, oud_abuse_study_cont)

write_rds(oud_abuse_clean, "data/final/oud_abuse.rds")

