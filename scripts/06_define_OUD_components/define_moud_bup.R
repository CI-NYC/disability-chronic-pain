################################################################################
################################################################################
### Define MOUD (buprenorphine)
### Author: Kat Hoffman July 2023
################################################################################
################################################################################

# Set up ------------------------------------------------------------------

# load libraries
library(arrow)
library(tidyverse)
library(tidylog)
library(lubridate)
library(data.table)
library(tictoc)
library(here)

proj_dir <- "projects/create_cohort"

# update BUP NDC list
# scp -r -i .ssh/kh3233-disabilityandpain-ubuntu COVID\ Research\ Dropbox/Kat\ Hoffman/GitHub/disability/projects/define_moud/input/best_list.rds kh3233@172.25.96.48:/home/kh3233/projects/define_moud/input/ 
# scp -r -i .ssh/kh3233-disabilityandpain-ubuntu COVID\ Research\ Dropbox/Kat\ Hoffman/GitHub/disability/projects/define_moud/input/cms_list.rds kh3233@172.25.96.48:/home/kh3233/projects/define_moud/input

best_list <- read_rds(here("projects/define_moud/input/best_list.rds"))

dts_cohorts <- open_dataset("projects/create_cohort/data/tafdedts/dts_cohorts.parquet") |>
    collect() |> 
    mutate(index = rep(1:32, length.out=n()))

td <- "/home/data/12201/" # directory of interest

# Read in RXL (pharmacy line)
files <- paste0(list.files(td, pattern = "TAFRXL", recursive = TRUE))
rxl <- open_dataset(paste0(td, files), format="parquet")

# read in OTL (other services line)
files <- paste0(list.files(td, pattern = "TAFOTL", recursive = TRUE))
otl <- open_dataset(paste0(td, files), format="parquet")


# Filter RXL files by cohort ---------------------------------------------------------

best_rxl <-
    rxl |>
    filter(NDC %in% best_list$ndc) |>
    select(BENE_ID, 
           NDC,
           CLM_ID,
           NDC_UOM_CD, 
           NDC_QTY,
           DAYS_SUPPLY,
           RX_FILL_DT) |>
    collect() 

# filter BUP scripts, merge BUP NDC-dose map to compute strength/day
best_rxl_all <-
    best_rxl |>
    left_join(best_list |> rename(NDC = ndc)) |>
    mutate(pills_per_day = NDC_QTY/DAYS_SUPPLY,
           strength_per_day = strength * pills_per_day)

# only keep relevant strengths/day of BUP scripts
best_rxl_clean <-
    best_rxl_all |>
    mutate(keep = case_when(check == 0 ~ 1, # keep  if don't need to check
                            check == 1 & strength_per_day >= 10 & strength_per_day < 50 ~ 1, # less than 10 counts towards probable misuse (and chronic pain)
                            TRUE ~ 0
    )) |>
    filter(keep == 1)  |>
    mutate(moud_end_dt = RX_FILL_DT + days(DAYS_SUPPLY + 21)) |> 
    select(BENE_ID, NDC, moud_med, form, moud_start_dt = RX_FILL_DT, moud_end_dt) 

write_rds(best_rxl_clean, here(proj_dir, "data/oud_info/bup/best_rxl_clean.rds"))


# Extract all of the scripts (NDC codes) in Other Services
tic()
best_otl <-
    otl |>
    filter(NDC %in% best_list$ndc) |>
    mutate(LINE_SRVC_BGN_DT = case_when(is.na(LINE_SRVC_BGN_DT) ~ LINE_SRVC_END_DT, TRUE ~ LINE_SRVC_BGN_DT)) |>
    select(BENE_ID,
           CLM_ID,
           NDC,
           NDC_UOM_CD, 
           NDC_QTY,
           LINE_SRVC_BGN_DT,
           LINE_SRVC_END_DT,
           LINE_PRCDR_CD,
           LINE_PRCDR_CD_SYS,
           ACTL_SRVC_QTY,
           ALOWD_SRVC_QTY)  |>
    collect() 
toc()

best_otl <- best_otl |> left_join(best_list |> rename(NDC = ndc)) # add best list of buprenorphine back into the full data

# save the bup injections (definitely MOUD) -- 30 days supply
best_injections_otl <-
    best_otl |>
    filter(form == "injection")|>
    select(BENE_ID, NDC, moud_med, form, moud_start_dt = LINE_SRVC_BGN_DT) |>
    mutate(moud_end_dt = moud_start_dt + days(30) + days(21))

# Bup-nal, definitely MOUD -- assuming 1 day supply
best_nal_otl <- 
    best_otl |>
    mutate(form %in% c("tablet","film"), check == 0) |>
    select(BENE_ID, NDC, moud_med, moud_start_dt = LINE_SRVC_BGN_DT, form) |>
    mutate(moud_end_dt =  moud_start_dt + days(21))

# not bup-nal, need to check
best_check_otl <- 
    best_otl |>
    mutate(form %in% c("tablet","film"), check == 1) 

# compute strength / day, ultimately will check if strength / day is > 10mg
best_checked_otl <-
    best_check_otl |>
    mutate(strength_times_quantity = case_when(NDC_UOM_CD == "UN" ~ strength * NDC_QTY, TRUE ~  strength)) |>
    group_by(BENE_ID, LINE_SRVC_BGN_DT) |>
    add_count() |>
    summarize(strength_per_day = sum(strength_times_quantity))

best_otl_over10mg <-
    best_checked_otl |>
    filter(strength_per_day >= 10) |>
    select(BENE_ID, moud_start_dt = LINE_SRVC_BGN_DT) |>
    mutate(moud_end_dt =  moud_start_dt + days(21),
           moud_med = "bup")

# pull HCPCS codes --------------------------------------------------------

best_hcpcs <- c(
    "J0570",	# Buprenorphine implant, 74.2 mg
    "J0572",	# Buprenorphine/naloxone, oral, less than or equal to 3 mg buprenorphine
    "J0573",	# Buprenorphine/naloxone, oral, greater than 3 mg, but less than or equal to 6 mg buprenorphine
    "J0574",	# Buprenorphine/naloxone, oral, greater than 6 mg, but less than or equal to 10 mg buprenorphine
    "J0575",	# Buprenorphine/naloxone, oral, greater than 10 mg buprenorphine
    "Q9991",	# Injection, buprenorphine extended-release (sublocade), less than or equal to 100 mg
    "Q9992"	# Injection, buprenorphine extended-release (sublocade), greater than 100 mg
    # None of the "G****" codes are picked up in the data.
    # "G2068",	# Medication assisted treatment, buprenorphine (oral); weekly bundle including dispensing and/or administration, substance use counseling, individual and group therapy, and toxicology testing if performed (provision of the services by a medicare-enrolled opioid treatment program)
    # "G2069",	# Medication assisted treatment, buprenorphine (injectable); weekly bundle including dispensing and/or administration, substance use counseling, individual and group therapy, and toxicology testing if performed (provision of the services by a medicare-enrolled opioid treatment program)
    # "G2070",	# Medication assisted treatment, buprenorphine (implant insertion); weekly bundle including dispensing and/or administration, substance use counseling, individual and group therapy, and toxicology testing if performed (provision of the services by a medicare-enrolled opioid treatment program)
    # "G2071",	# Medication assisted treatment, buprenorphine (implant removal); weekly bundle including dispensing and/or administration, substance use counseling, individual and group therapy, and toxicology testing if performed (provision of the services by a medicare-enrolled opioid treatment program) 
    # "G2072",	# Medication assisted treatment, buprenorphine (implant insertion and removal); weekly bundle including dispensing and/or administration, substance use counseling, individual and group therapy, and toxicology testing if performed (provision of the services by a medicare-enrolled opioid treatment program)
    # "G2079" 	# Take-home supply of buprenorphine (oral); up to 7 additional day supply (provision of the services by a medicare-enrolled opioid treatment program); list separately in addition to code for primary procedure
)

best_hcpcs_otl <-
    otl |>
    filter(LINE_PRCDR_CD %in% best_hcpcs) |>
    mutate(LINE_SRVC_BGN_DT = case_when(is.na(LINE_SRVC_BGN_DT) ~ LINE_SRVC_END_DT, TRUE ~ LINE_SRVC_BGN_DT),
    ) |>
    select(BENE_ID,
           STATE_CD, 
           NDC,
           NDC_UOM_CD, 
           NDC_QTY,
           LINE_SRVC_BGN_DT,
           LINE_SRVC_END_DT,
           LINE_PRCDR_CD,
           LINE_PRCDR_CD_SYS,
           ACTL_SRVC_QTY,
           ALOWD_SRVC_QTY) |>
    collect()

best_hcpcs_otl |> count(LINE_PRCDR_CD)

bup_hcpcs_otl <-
    best_hcpcs_otl |>
    mutate(moud_med = "bup",
           form = case_when(LINE_PRCDR_CD == "J0570" ~ "implant",
                            str_detect(LINE_PRCDR_CD, "Q") ~ "injection",
                            str_detect(LINE_PRCDR_CD, "J") ~ "tablet"),
           moud_start_dt = LINE_SRVC_BGN_DT,
           moud_end_dt = case_when(form == "implant" ~ moud_start_dt + 21 + 182,
                                   form == "injection" ~ moud_start_dt + 21 + 30,
                                   form == "tablet" ~ moud_start_dt + 21 
                                   )) |> # bup implants last 6 months
    select(BENE_ID, moud_med, form, moud_start_dt, moud_end_dt)
    
# Combine all sources of bup moud
best_all_bup <- full_join(best_injections_otl, best_nal_otl) |>
    full_join(best_otl_over10mg) |>
    full_join(best_rxl_clean) |>
    full_join(bup_hcpcs_otl) |>
    distinct(BENE_ID, moud_start_dt, moud_end_dt) |>
    group_by(BENE_ID) |>
    arrange(BENE_ID, moud_start_dt)

# adjudicate the buprenorphine information depending on when the beneficiary received the next dose
# check when the next/last injection was given, and how long that was since the last injection ended (injection + 28 days)
best_all_bup_adj <-
    best_all_bup |>
    drop_na(BENE_ID) |>
    mutate(lag_moud_end_dt = lag(moud_end_dt), # when did the last bup end?
           days_since_last_moud = as.integer(difftime(moud_start_dt, lag_moud_end_dt, units="days")),
           lead_moud_start_dt = lead(moud_start_dt), # when is the next dose starting compared to last moud end date
           days_to_next_moud = as.integer(difftime(lead_moud_start_dt, moud_end_dt, units="days")))  |>
    ungroup()|>
    # indicator of whether to use the moud_start_dt/moud_end_dt or ignore them
    mutate(use_start_dt = case_when(is.na(days_since_last_moud) ~ 1,
                                    days_since_last_moud > 0 ~ 1,
                                    TRUE ~ 0),
           
           use_end_dt = case_when(is.na(days_to_next_moud) ~ 1,
                                  days_to_next_moud > 0 ~ 1,
                                  TRUE ~ 0))

# keep only the needed rows for start dates
best_all_bup_start_dts <-
    best_all_bup_adj |>
    filter(use_start_dt == 1) |>
    select(BENE_ID, moud_start_dt) |>
    ungroup()

# keep only the needed rows for end dates (same N and order as all_bup_start_dts)
best_all_bup_end_dts <-
    best_all_bup_adj |>
    filter(use_end_dt == 1) |>
    select(BENE_ID, moud_end_dt) |>
    ungroup()

# merge to one final bup start/stop data set
best_all_bup_start_stop <-
    best_all_bup_start_dts |>
    bind_cols(best_all_bup_end_dts |> select(-BENE_ID)) |>
    mutate(moud_med = "bup") |>
    select(BENE_ID, moud_med, everything())|>
    left_join(dts_cohorts |> select(BENE_ID, washout_start_dt)) |>
    filter(!(moud_end_dt < washout_start_dt)) |> # filter out rows that end before washout period begins
    select(-washout_start_dt)


# BUP
best_bup_intervals <-
    best_all_bup_start_stop |>
    left_join(dts_cohorts) |> # left_join, only merge with rows that have any bup
    # start or end date can fall in the periods of interst
    mutate(moud_start_dt = case_when(moud_start_dt < washout_start_dt ~ washout_start_dt, TRUE ~ moud_start_dt),
           oud_moud_bup_washout_cal = case_when(moud_start_dt %within% interval(washout_start_dt, washout_cal_end_dt) ~ 1,
                                                moud_end_dt %within% interval(washout_start_dt, washout_cal_end_dt) ~ 1,
                                                TRUE ~ 0),
           oud_moud_bup_washout_12mos_cal = case_when(moud_start_dt %within% interval(washout_start_dt, washout_12mos_end_dt) ~ 1,
                                                      moud_end_dt %within% interval(washout_start_dt, washout_12mos_end_dt) ~ 1,
                                                      TRUE ~ 0),
           oud_moud_bup_washout_cont = case_when(moud_start_dt %within% interval(washout_start_dt, washout_cont_end_dt) ~ 1,
                                                 moud_end_dt %within% interval(washout_start_dt, washout_cont_end_dt) ~ 1,
                                                 TRUE ~ 0),
           oud_moud_bup_study_cal = case_when(moud_start_dt %within% interval(washout_start_dt, study_cal_end_dt) ~ 1,
                                              moud_end_dt %within% interval(washout_start_dt, study_cal_end_dt) ~ 1,
                                              TRUE ~ 0),
           oud_moud_bup_study_cont = case_when(moud_start_dt %within% interval(washout_start_dt, study_cont_end_dt) ~ 1,
                                               moud_end_dt %within% interval(washout_start_dt, study_cont_end_dt) ~ 1,
                                               TRUE ~ 0),
    ) |>
    group_by(BENE_ID) |>
    summarize(across(starts_with("oud_moud"), max)) # keep a row for each BENE_ID that denotes whether they had the drug in this period

all_bup_intervals <- 
    dts_cohorts |>
    select(BENE_ID) |>
    left_join(best_bup_intervals) |>
    mutate(across(contains("oud_moud"), ~ifelse(is.na(.x), 0, .x))) # replace missing data with zero (no bup in that interval)

write_rds(all_bup_intervals, "projects/create_cohort/data/oud_info/bup/all_bup_intervals.rds")


