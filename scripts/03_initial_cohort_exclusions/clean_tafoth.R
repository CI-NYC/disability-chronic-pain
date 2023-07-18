################################################################################
################################################################################
###  CLEAN OTHER SERVICES FILES
###  Kat Hoffman, Jan 2023, updated March 2023
###  Purpose: clean TAFOTH files for exclusion criteria 
###  Output: two indicator files data/tafoth/oth_cohort_exclusions_*.rds for each
####        cohort documenting exclusion criteria (cohort_exclusion_* columns)
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

source("scripts/ICD_codes/disabilities.R")

td <- "/home/data/12201/" # directory of interest
oth_files <- paste0(list.files(td, pattern = "*TAFOTH*", recursive = TRUE)) # files of interest
oth <- open_dataset(paste0(td, oth_files), format="parquet") # arrow dataset

dts_cohorts <- open_dataset("data/tafdedts/dts_cohorts.parquet") |>
    collect() |> 
    mutate(index = rep(1:25, length.out=n()))

# first_iter <- dts_cohorts |> filter(index == 1) |> pull(BENE_ID)
# 
# icd_codes_to_check <-
#   oth |>
#   filter(BENE_ID %in% first_iter) |>
#   select(BENE_ID, SRVC_BGN_DT, contains("DGNS_CD")) |>
#   collect()

# 300+ million row file so work on just one DG_CD at a time for now
# 40 million rows after removing NAs and then filter out washout period relevant codes

all <- 1:25
td <- "data/tafoth/tmp_splits_exclusions/" 
files <- paste0(list.files(td, pattern = "*.parquet", recursive = TRUE)) 
done <- unique(parse_number(files))
still <- all[-which(all %in% done)]

foreach(i = still) %dopar%
    {
    iter <- dts_cohorts |> filter(index == i) |> pull(BENE_ID)
    print(paste(i, Sys.time()))
    icd_codes_to_check <-
        oth |>
        filter(BENE_ID %in% iter) |>
        select(BENE_ID, SRVC_BGN_DT, SRVC_END_DT, contains("DGNS_CD")) |>
        collect()
    # split up by Diagnosis codes (first, 1) because data is so long
    oth_dgcd1_cal <-
        icd_codes_to_check |> 
        select(BENE_ID, SRVC_BGN_DT, SRVC_END_DT, DGNS_CD_1) |>
        drop_na() |>
        inner_join(dts_cohorts |> select(BENE_ID, washout_start_dt, washout_cal_end_dt)) |>
        mutate(SRVC_BGN_DT = case_when(is.na(SRVC_BGN_DT) ~ SRVC_END_DT, TRUE ~ SRVC_BGN_DT)) |>
        filter(SRVC_BGN_DT %within% interval(washout_start_dt, washout_cal_end_dt))
    print(paste(i, Sys.time()))
    # adjudicate ICD codes by disqualifications - add oud later
    oth_dgcd1_cal_clean <-
        oth_dgcd1_cal |>
        rename(dgcd = DGNS_CD_1) |>
        mutate(cohort_exclusion_cal_itlcdsbl = dgcd %in% itlcdsbl_icds,
               cohort_exclusion_cal_dementia =  dgcd %in% dementia_icds,
               cohort_exclusion_cal_schiz =  dgcd %in% schiz_icds,
               cohort_exclusion_cal_speech =  dgcd %in% speech_icds,
               cohort_exclusion_cal_cerpals =  dgcd %in% cerpals_icds,
               cohort_exclusion_cal_epilepsy =  dgcd %in% epilepsy_icds,
               cohort_exclusion_cal_deaf_oth =  dgcd %in% deafness_icds,
               cohort_exclusion_cal_blind_oth =  dgcd %in% blindness_icds,
               cohort_exclusion_cal_pall = dgcd %in% "Z515", # palliative care encounter
               cohort_exclusion_cal_cancer =  dgcd %in% cancer_icds
        )
    write_parquet(oth_dgcd1_cal_clean, paste0("data/tafoth/tmp_splits_exclusions/", i, "_dgcd1_cal.parquet"))
    print(paste(i, Sys.time())) # now clean second Diagnosis code
    oth_dgcd2_cal <-
        icd_codes_to_check |> 
        select(BENE_ID, SRVC_BGN_DT, SRVC_END_DT, DGNS_CD_2) |>
        drop_na() |>
        inner_join(dts_cohorts |> select(BENE_ID, washout_start_dt, washout_cal_end_dt)) |>
        mutate(SRVC_BGN_DT = case_when(is.na(SRVC_BGN_DT) ~ SRVC_END_DT, TRUE ~ SRVC_BGN_DT)) |>
        filter(SRVC_BGN_DT %within% interval(washout_start_dt, washout_cal_end_dt))
    print(paste(i, Sys.time()))
    # adjudicate ICD codes by disqualifications
    oth_dgcd2_cal_clean <-
        oth_dgcd2_cal |>
        rename(dgcd = DGNS_CD_2) |>
        mutate(cohort_exclusion_cal_itlcdsbl = dgcd %in% itlcdsbl_icds,
               cohort_exclusion_cal_dementia =  dgcd %in% dementia_icds,
               cohort_exclusion_cal_schiz =  dgcd %in% schiz_icds,
               cohort_exclusion_cal_speech =  dgcd %in% speech_icds,
               cohort_exclusion_cal_cerpals =  dgcd %in% cerpals_icds,
               cohort_exclusion_cal_epilepsy =  dgcd %in% epilepsy_icds,
               cohort_exclusion_cal_deaf_oth =  dgcd %in% deafness_icds,
               cohort_exclusion_cal_blind_oth =  dgcd %in% blindness_icds,
               cohort_exclusion_cal_pall = dgcd %in% "Z515", # palliative care encounter
               cohort_exclusion_cal_cancer =  dgcd %in% cancer_icds
        )
    write_parquet(oth_dgcd2_cal_clean, paste0("data/tafoth/tmp_splits_exclusions/", i, "_dgcd2_cal.parquet"))
    }



##### 12 months washout period
all <- 1:25
# td <- "data/tafoth/tmp_splits_exclusions/" 
# files <- paste0(list.files(td, pattern = "*.parquet", recursive = TRUE)) 
# done <- unique(parse_number(files))
still <- c(2, 5, 6, 8, 11, 13, 15, 16, 17, 22, 25)

foreach(i = still) %dopar%
    {
        iter <- dts_cohorts |> filter(index == i) |> pull(BENE_ID)
        print(paste(i, Sys.time()))
        icd_codes_to_check <-
            oth |>
            filter(BENE_ID %in% iter) |>
            select(BENE_ID, SRVC_BGN_DT, SRVC_END_DT, contains("DGNS_CD")) |>
            collect()
        # split up by Diagnosis codes (first, 1) because data is so long
        oth_dgcd1_cal <-
            icd_codes_to_check |> 
            select(BENE_ID, SRVC_BGN_DT, SRVC_END_DT, DGNS_CD_1) |>
            drop_na() |>
            inner_join(dts_cohorts |> select(BENE_ID, washout_start_dt, washout_12mos_end_dt)) |>
            mutate(SRVC_BGN_DT = case_when(is.na(SRVC_BGN_DT) ~ SRVC_END_DT, TRUE ~ SRVC_BGN_DT)) |>
            filter(SRVC_BGN_DT %within% interval(washout_start_dt, washout_12mos_end_dt))
        print(paste(i, Sys.time()))
        # adjudicate ICD codes by disqualifications - add oud later
        oth_dgcd1_cal_clean <-
            oth_dgcd1_cal |>
            rename(dgcd = DGNS_CD_1) |>
            mutate(cohort_exclusion_cal_itlcdsbl = dgcd %in% itlcdsbl_icds,
                   cohort_exclusion_cal_dementia =  dgcd %in% dementia_icds,
                   cohort_exclusion_cal_schiz =  dgcd %in% schiz_icds,
                   cohort_exclusion_cal_speech =  dgcd %in% speech_icds,
                   cohort_exclusion_cal_cerpals =  dgcd %in% cerpals_icds,
                   cohort_exclusion_cal_epilepsy =  dgcd %in% epilepsy_icds,
                   cohort_exclusion_cal_deaf_oth =  dgcd %in% deafness_icds,
                   cohort_exclusion_cal_blind_oth =  dgcd %in% blindness_icds,
                   cohort_exclusion_cal_pall = dgcd %in% "Z515", # palliative care encounter
                   cohort_exclusion_cal_cancer =  dgcd %in% cancer_icds
            )
        write_parquet(oth_dgcd1_cal_clean, paste0("data/tafoth/tmp_splits_exclusions/", i, "_dgcd1_12mos_cal.parquet"))
        print(paste(i, Sys.time())) # now clean second Diagnosis code
        oth_dgcd2_cal <-
            icd_codes_to_check |> 
            select(BENE_ID, SRVC_BGN_DT, SRVC_END_DT, DGNS_CD_2) |>
            drop_na() |>
            inner_join(dts_cohorts |> select(BENE_ID, washout_start_dt, washout_12mos_end_dt)) |>
            mutate(SRVC_BGN_DT = case_when(is.na(SRVC_BGN_DT) ~ SRVC_END_DT, TRUE ~ SRVC_BGN_DT)) |>
            filter(SRVC_BGN_DT %within% interval(washout_start_dt, washout_12mos_end_dt))
        print(paste(i, Sys.time()))
        # adjudicate ICD codes by disqualifications
        oth_dgcd2_cal_clean <-
            oth_dgcd2_cal |>
            rename(dgcd = DGNS_CD_2) |>
            mutate(cohort_exclusion_cal_itlcdsbl = dgcd %in% itlcdsbl_icds,
                   cohort_exclusion_cal_dementia =  dgcd %in% dementia_icds,
                   cohort_exclusion_cal_schiz =  dgcd %in% schiz_icds,
                   cohort_exclusion_cal_speech =  dgcd %in% speech_icds,
                   cohort_exclusion_cal_cerpals =  dgcd %in% cerpals_icds,
                   cohort_exclusion_cal_epilepsy =  dgcd %in% epilepsy_icds,
                   cohort_exclusion_cal_deaf_oth =  dgcd %in% deafness_icds,
                   cohort_exclusion_cal_blind_oth =  dgcd %in% blindness_icds,
                   cohort_exclusion_cal_pall = dgcd %in% "Z515", # palliative care encounter
                   cohort_exclusion_cal_cancer =  dgcd %in% cancer_icds
            )
        write_parquet(oth_dgcd2_cal_clean, paste0("data/tafoth/tmp_splits_exclusions/", i, "_dgcd2_12mos_cal.parquet"))
    }


#### Continous enorlment cohort
still <- c(7, 8, 13, 14, 15, 16, 19, 20, 25)
foreach(i = still) %dopar% {
    iter <- dts_cohorts |> filter(index == i) |> pull(BENE_ID)
    print(paste(i, Sys.time()))
    icd_codes_to_check <-
        oth |>
        filter(BENE_ID %in% iter) |>
        select(BENE_ID, SRVC_BGN_DT, SRVC_END_DT, contains("DGNS_CD")) |>
        collect()
    print(paste(i, Sys.time()))
    oth_dgcd1_cont <-
        icd_codes_to_check |> 
        select(BENE_ID, SRVC_BGN_DT,  SRVC_END_DT, DGNS_CD_1) |>
        drop_na() |>
        inner_join(dts_cohorts |> select(BENE_ID, washout_start_dt, washout_cont_end_dt)) |>
        mutate(SRVC_BGN_DT = case_when(is.na(SRVC_BGN_DT) ~ SRVC_END_DT, TRUE ~ SRVC_BGN_DT)) |>
        filter(SRVC_BGN_DT %within% interval(washout_start_dt, washout_cont_end_dt))
    # adjudicate ICD codes by disqualifications - add oud later
    oth_dgcd1_cont_clean <-
        oth_dgcd1_cont |>
        rename(dgcd = DGNS_CD_1) |>
        mutate(cohort_exclusion_cont_itlcdsbl = dgcd %in% itlcdsbl_icds,
               cohort_exclusion_cont_dementia =  dgcd %in% dementia_icds,
               cohort_exclusion_cont_schiz =  dgcd %in% schiz_icds,
               cohort_exclusion_cont_speech =  dgcd %in% speech_icds,
               cohort_exclusion_cont_cerpals =  dgcd %in% cerpals_icds,
               cohort_exclusion_cont_epilepsy =  dgcd %in% epilepsy_icds,
               cohort_exclusion_cont_deaf_oth =  dgcd %in% deafness_icds,
               cohort_exclusion_cont_blind_oth =  dgcd %in% blindness_icds,
               cohort_exclusion_cont_pall = dgcd %in% "Z515", # palliative care encounter
               cohort_exclusion_cont_cancer =  dgcd %in% cancer_icds
        )
    write_parquet(oth_dgcd1_cont_clean, paste0("data/tafoth/tmp_splits_exclusions/", i, "_dgcd1_cont.parquet"))
    print(paste(i, Sys.time()))
    oth_dgcd2_cont <-
        icd_codes_to_check |> 
        select(BENE_ID, SRVC_BGN_DT, SRVC_END_DT, DGNS_CD_2) |>
        drop_na() |>
        inner_join(dts_cohorts |> select(BENE_ID, washout_start_dt, washout_cont_end_dt)) |>
        mutate(SRVC_BGN_DT = case_when(is.na(SRVC_BGN_DT) ~ SRVC_END_DT, TRUE ~ SRVC_BGN_DT)) |>
        filter(SRVC_BGN_DT %within% interval(washout_start_dt, washout_cont_end_dt))
    print(paste(i, Sys.time()))
    # adjudicate ICD codes by disqualifications
    oth_dgcd2_cont_clean <-
        oth_dgcd2_cont |>
        rename(dgcd = DGNS_CD_2) |>
        mutate(cohort_exclusion_cont_itlcdsbl = dgcd %in% itlcdsbl_icds,
               cohort_exclusion_cont_dementia =  dgcd %in% dementia_icds,
               cohort_exclusion_cont_schiz =  dgcd %in% schiz_icds,
               cohort_exclusion_cont_speech =  dgcd %in% speech_icds,
               cohort_exclusion_cont_cerpals =  dgcd %in% cerpals_icds,
               cohort_exclusion_cont_epilepsy =  dgcd %in% epilepsy_icds,
               cohort_exclusion_cont_deaf_oth =  dgcd %in% deafness_icds,
               cohort_exclusion_cont_blind_oth =  dgcd %in% blindness_icds,
               cohort_exclusion_cont_pall = dgcd %in% "Z515", # palliative care encounter
               cohort_exclusion_cont_cancer =  dgcd %in% cancer_icds
        )
    write_parquet(oth_dgcd2_cont_clean, paste0("data/tafoth/tmp_splits_exclusions/", i, "_dgcd2_cont.parquet"))
}

# Gather all the splits and create a single data frame to save
td <- "data/tafoth/tmp_splits_exclusions/" 
files <- paste0(list.files(td, pattern = "*cont.parquet", recursive = TRUE)) 
dts_dgcd_cont <- open_dataset(paste0(td, files), format="parquet") # arrow dataset

dts_dgcd_cont_collect <- dts_dgcd_cont |> select(BENE_ID, starts_with("cohort")) |> distinct() |> collect() # 11 million rows
dts_dgcd_cont_clean <- 
    dts_dgcd_cont_collect |>
    group_by(BENE_ID) |>
    summarize(across(starts_with("cohort"), ~ifelse(sum(.x) >= 1, 1, 0))) |>
    rename()
write_rds(dts_dgcd_cont_clean, "data/tafoth/oth_cohort_exclusions_cont.rds")

files <- paste0(list.files(td, pattern = "*cal.parquet", recursive = TRUE))  ### REMOVE RTHE ONES THAT HAVE 12 months in them
dts_dgcd_cal <- open_dataset(paste0(td, files), format="parquet") # arrow dataset
dts_dgcd_cal_collect <- dts_dgcd_cal |> select(BENE_ID, starts_with("cohort")) |> distinct() |> collect() # 11 million rows
dts_dgcd_cal_clean <- 
    dts_dgcd_cal_collect |>
    group_by(BENE_ID) |>
    summarize(across(starts_with("cohort"), ~ifelse(sum(.x) >= 1, 1, 0))) 
write_rds(dts_dgcd_cal_clean, "data/tafoth/oth_cohort_exclusions_cal.rds")


### REDONE LATER
td <- "data/tafoth/tmp_splits_exclusions/" 
files <- paste0(list.files(td, pattern = "*12mos_cal.parquet", recursive = TRUE))  ### REMOVE RTHE ONES THAT HAVE 12 months in them
dts_dgcd_cal <- open_dataset(paste0(td, files), format="parquet") # arrow dataset
dts_dgcd_cal_collect <- dts_dgcd_cal |> 
    select(BENE_ID, starts_with("cohort")) |>
    distinct() |>
    collect()  |>
    rename_all( ~ str_replace(., "_cal", "_12mos_cal")) 
dts_dgcd_cal_clean <- 
    dts_dgcd_cal_collect |>
    group_by(BENE_ID) |>
    summarize(across(starts_with("cohort"), ~ifelse(sum(.x) >= 1, 1, 0))) 
write_rds(dts_dgcd_cal_clean, "data/tafoth/oth_cohort_exclusions_12mos_cal.rds")


