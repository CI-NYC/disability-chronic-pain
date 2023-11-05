################################################################################
################################################################################
###  Extract a variable for appendectomy for a negative control
###  Kat Hoffman, Sept 2023
################################################################################
################################################################################

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

dts_cohorts <- open_dataset("../data/clean/create_cohort/tafdedts/dts_cohorts.parquet") |>
    collect() 

dat_lmtp <- read_rds("projects/create_cohort/data/final/dat_lmtp.rds") 


td <- "/home/data/12201/" # directory of interest

iph_files <- paste0(list.files(td, pattern = "*TAFIPH*", recursive = TRUE)) # files of interest
iph <- open_dataset(paste0(td, iph_files), format="parquet") # arrow dataset

# read in OTH (other services baseline)
files <- paste0(list.files(td, pattern = "TAFOTH", recursive = TRUE))
oth <- open_dataset(paste0(td, files), format="parquet")



# ear infection -----------------------------------------------------------


dg1_ear <-
    oth |>
    filter(DGNS_CD_1 == "H6691") |>
    mutate(dt = case_when(is.na(SRVC_BGN_DT) ~ SRVC_END_DT, TRUE ~ SRVC_BGN_DT)) |>
    select(BENE_ID, dt) |>
    collect()

dg2_ear <-
    oth |>
    filter(DGNS_CD_2 == "H6691") |>
    mutate(dt = case_when(is.na(SRVC_BGN_DT) ~ SRVC_END_DT, TRUE ~ SRVC_BGN_DT)) |>
    select(BENE_ID, dt) |>
    collect()

dg_ear <- bind_rows(dg1_ear, dg2_ear) 

rel_ear_dg <-
    dts_cohorts |>
    select(BENE_ID, washout_start_dt, study_cal_end_dt) |>
    left_join(dg_ear) |>
    mutate(ear_infection = case_when(dt %within% interval(washout_start_dt, study_cal_end_dt) ~ 1,
                                TRUE ~ 0)) |>
    filter(ear_infection == 1) |>
    distinct(BENE_ID, .keep_all = T)

dat_ear <-
    dat_lmtp |>
    left_join(rel_ear_dg |> select(BENE_ID, ear_infection)) |>
    mutate(ear_infection = case_when(ear_infection == 1 ~ 1,
                                TRUE ~ 0),
           complete2_ear = case_when(complete == 1 ~ 1, # if study is complete, 1
                                     ear_infection == 1 ~ 1, # or, if they have the event, 1
                                      TRUE ~ 0), # otherwise incomplete
           ear_event = case_when(complete2_ear == 1 ~ ear_infection,
                                  TRUE ~ NA_real_))



saveRDS(dat_ear, "projects/create_cohort/data/final/dat_lmtp_ear_infection.rds")


# appendicitis ------------------------------------------------------------

app_dg <-
    iph |>
    select(BENE_ID, ADMSN_DT, ADMTG_DGNS_CD) |>
    filter(str_detect(ADMTG_DGNS_CD, "K35")) |>
    collect() 


rel_app_washout <-
    dts_cohorts |>
    select(BENE_ID, washout_start_dt, washout_cal_end_dt) |>
    left_join(app_dg) |>
    mutate(appendix = case_when(ADMSN_DT %within% interval(washout_start_dt, washout_cal_end_dt) ~ 1,
                                TRUE ~ 0)) |>
    filter(appendix == 1) |>
    distinct(BENE_ID, .keep_all = T)
    
dat_app_exp <-
    dat_lmtp |>
    left_join(rel_app_washout) |>
    mutate(appendix_washout = case_when(appendix == 1 ~ 1, TRUE ~ 0))

saveRDS(dat_app_exp, "projects/create_cohort/data/final/dat_lmtp_negcontrol_exposure.rds")


rel_app_dg <-
    dts_cohorts |>
    select(BENE_ID, washout_start_dt, study_cal_end_dt) |>
    left_join(app_dg) |>
    mutate(appendix = case_when(ADMSN_DT %within% interval(study_cal_start_dt, study_cal_end_dt) ~ 1,
                                       TRUE ~ 0)) |>
    filter(appendix == 1) |>
    distinct(BENE_ID, .keep_all = T)

dat_app <-
    dat_lmtp |>
    left_join(rel_app_dg |> select(BENE_ID, appendix)) |>
    mutate(appendix = case_when(appendix == 1 ~ 1,
                                TRUE ~ 0),
           # complete_asph = case_when(is.na(censoring_ever_dt) ~ 1, TRUE ~ 0),
           ### OUD CAL
           complete2_app = case_when(complete == 1 ~ 1, # if study is complete, 1
                                      appendix == 1 ~ 1, # or, if they have the event, 1
                                      TRUE ~ 0), # otherwise incomplete
           app_event = case_when(complete2_app == 1 ~ appendix,
                                  TRUE ~ NA_real_))

saveRDS(dat_app, "projects/create_cohort/data/final/dat_lmtp_negcontrol.rds")


# asphyxia ----------------------------------------------------------------


dg1 <-
    oth |>
    filter(DGNS_CD_1 == "T780") |>
    mutate(dt = case_when(is.na(SRVC_BGN_DT) ~ SRVC_END_DT, TRUE ~ SRVC_BGN_DT)) |>
    select(BENE_ID, dt) |>
    collect()

dg2 <-
    oth |>
    filter(DGNS_CD_2 == "H6691") |>
    mutate(dt = case_when(is.na(SRVC_BGN_DT) ~ SRVC_END_DT, TRUE ~ SRVC_BGN_DT)) |>
    select(BENE_ID, dt) |>
    collect()

dg <- bind_rows(dg1, dg2) 

rel_dg <-
    dts_cohorts |>
    select(BENE_ID, washout_start_dt, study_cal_end_dt) |>
    left_join(dg) |>
    mutate(asph = case_when(dt %within% interval(washout_start_dt, study_cal_end_dt) ~ 1,
                                     TRUE ~ 0)) |>
    filter(asph == 1) |>
    distinct(BENE_ID, .keep_all = T)

dat_asph <-
    dat_lmtp |>
    left_join(rel_dg |> select(BENE_ID, asph)) |>
    mutate(
        asph = case_when(asph == 1 ~ 1, TRUE ~ 0),
        # complete_asph = case_when(is.na(censoring_ever_dt) ~ 1, TRUE ~ 0),
        ### OUD CAL
        complete2_asph = case_when(complete == 1 ~ 1, # if study is complete, 1
                                  asph == 1 ~ 1, # or, if they have the event, 1
                                  TRUE ~ 0), # otherwise incomplete
        asph_event = case_when(complete2_asph == 1 ~ asph,
                                  TRUE ~ NA_real_)) 

saveRDS(dat_asph, "projects/create_cohort/data/final/dat_lmtp_asph.rds")



