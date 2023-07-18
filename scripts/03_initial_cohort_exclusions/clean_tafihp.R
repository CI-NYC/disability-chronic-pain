################################################################################
################################################################################
###  CLEAN INPATIENT HOSPITAL FILES
###  Kat Hoffman, Dec 2022, updated March 2023
###  Purpose: clean TAFIHP files for exclusion criteria 
###  Output: two indicator files data/tafiph/iph_cohort_exclusions_*.rds for each
####        cohort documenting exclusion criteria (cohort_exclusion_* columns)
################################################################################
################################################################################

library(arrow)
library(tidyverse)
library(lubridate)
library(data.table)
library(furrr)
library(tidylog)
library(tictoc)

# read in tafihp data base (all years)
td <- "/home/data/12201/" # directory of interest
iph_files <- paste0(list.files(td, pattern = "*TAFIPH*", recursive = TRUE)) # files of interest
iph <- open_dataset(paste0(td, iph_files), format="parquet") # arrow dataset

dts_cohorts <- open_dataset("data/tafdedts/dts_cohorts.parquet") |>
    collect()

icd_codes_to_check <-
  iph |>
  select(BENE_ID, SRVC_BGN_DT, SRVC_END_DT, contains("DGNS_CD")) |>
  collect()

# obtain the date for all inpatient hospitalization services within hospital period
all_iph_icds_in_washout_cal <-
  icd_codes_to_check |>
  inner_join(dts_cohorts |> select(BENE_ID, washout_start_dt, washout_cal_end_dt)) |>
  mutate(SRVC_BGN_DT = case_when(is.na(SRVC_BGN_DT) ~ SRVC_END_DT, TRUE ~ SRVC_BGN_DT)) |>
  filter(SRVC_BGN_DT %within% interval(washout_start_dt, washout_cal_end_dt))

# obtain the date for all inpatient hospitalization services within 12 month washout period
all_iph_icds_in_washout_12mos_cal <-
    icd_codes_to_check |>
    inner_join(dts_cohorts |> select(BENE_ID, washout_start_dt, washout_12mos_end_dt)) |>
    mutate(SRVC_BGN_DT = case_when(is.na(SRVC_BGN_DT) ~ SRVC_END_DT, TRUE ~ SRVC_BGN_DT)) |>
    filter(SRVC_BGN_DT %within% interval(washout_start_dt, washout_12mos_end_dt))

# obtain the date for all inpatient hospitalization services within hospital period
all_iph_icds_in_washout_cont <-
    icd_codes_to_check |>
    inner_join(dts_cohorts |> select(BENE_ID, washout_start_dt, washout_cont_end_dt)) |>
    mutate(SRVC_BGN_DT = case_when(is.na(SRVC_BGN_DT) ~ SRVC_END_DT, TRUE ~ SRVC_BGN_DT)) |>
    filter(SRVC_BGN_DT %within% interval(washout_start_dt, washout_cont_end_dt))

##########################
##########################

source("scripts/ICD_codes/disabilities.R")

## Identify whether exclusion ICD code of interest occurs in washout ICDs

tic()
# Calendar enrollment cohort
icds_adj_cal <-
  all_iph_icds_in_washout_cal |>
  mutate(cohort_exclusion_cal_itlcdsbl_iph = +(if_any(starts_with("DGNS_CD"),  ~. %in% itlcdsbl_icds)),
         cohort_exclusion_cal_dementia_iph = +(if_any(starts_with("DGNS_CD"),  ~. %in% dementia_icds)),
         cohort_exclusion_cal_schiz_iph = +(if_any(starts_with("DGNS_CD"),  ~. %in% schiz_icds)),
         cohort_exclusion_cal_speech_iph = +(if_any(starts_with("DGNS_CD"),  ~. %in% speech_icds)),
         cohort_exclusion_cal_cerpals_iph = +(if_any(starts_with("DGNS_CD"),  ~. %in% cerpals_icds)),
         cohort_exclusion_cal_epilepsy_iph = +(if_any(starts_with("DGNS_CD"),  ~. %in% epilepsy_icds)),
         cohort_exclusion_cal_deaf_iph = +(if_any(starts_with("DGNS_CD"),  ~. %in% deafness_icds)), # need to combine these with dems
         cohort_exclusion_cal_blind_iph = +(if_any(starts_with("DGNS_CD"),  ~. %in% blindness_icds)), # need to combine these with dems
         cohort_exclusion_cal_pall_iph = +(if_any(starts_with("DGNS_CD"),  ~. %in% "Z515")), # palliative care encounter
         cohort_exclusion_cal_cancer_iph = +(if_any(contains("DGNS_CD"),  ~. %in% cancer_icds)) 
         )
toc()


tic()
# Calendar enrollment cohort -- 12 month washout
icds_adj_12mos_cal <-
    all_iph_icds_in_washout_12mos_cal |>
    mutate(cohort_exclusion_12mos_cal_itlcdsbl_iph = +(if_any(starts_with("DGNS_CD"),  ~. %in% itlcdsbl_icds)),
           cohort_exclusion_12mos_cal_dementia_iph = +(if_any(starts_with("DGNS_CD"),  ~. %in% dementia_icds)),
           cohort_exclusion_12mos_cal_schiz_iph = +(if_any(starts_with("DGNS_CD"),  ~. %in% schiz_icds)),
           cohort_exclusion_12mos_cal_speech_iph = +(if_any(starts_with("DGNS_CD"),  ~. %in% speech_icds)),
           cohort_exclusion_12mos_cal_cerpals_iph = +(if_any(starts_with("DGNS_CD"),  ~. %in% cerpals_icds)),
           cohort_exclusion_12mos_cal_epilepsy_iph = +(if_any(starts_with("DGNS_CD"),  ~. %in% epilepsy_icds)),
           cohort_exclusion_12mos_cal_deaf_iph = +(if_any(starts_with("DGNS_CD"),  ~. %in% deafness_icds)), # need to combine these with dems
           cohort_exclusion_12mos_cal_blind_iph = +(if_any(starts_with("DGNS_CD"),  ~. %in% blindness_icds)), # need to combine these with dems
           cohort_exclusion_12mos_cal_pall_iph = +(if_any(starts_with("DGNS_CD"),  ~. %in% "Z515")), # palliative care encounter
           cohort_exclusion_12mos_cal_cancer_iph = +(if_any(contains("DGNS_CD"),  ~. %in% cancer_icds)) 
    )
toc()

# Continuous enrollment cohort
icds_adj_cont <-
    all_iph_icds_in_washout_cont |>
    mutate(cohort_exclusion_cont_itlcdsbl_iph = +(if_any(starts_with("DGNS_CD"),  ~. %in% itlcdsbl_icds)),
           cohort_exclusion_cont_dementia_iph = +(if_any(starts_with("DGNS_CD"),  ~. %in% dementia_icds)),
           cohort_exclusion_cont_schiz_iph = +(if_any(starts_with("DGNS_CD"),  ~. %in% schiz_icds)),
           cohort_exclusion_cont_speech_iph = +(if_any(starts_with("DGNS_CD"),  ~. %in% speech_icds)),
           cohort_exclusion_cont_cerpals_iph = +(if_any(starts_with("DGNS_CD"),  ~. %in% cerpals_icds)),
           cohort_exclusion_cont_epilepsy_iph = +(if_any(starts_with("DGNS_CD"),  ~. %in% epilepsy_icds)),
           cohort_exclusion_cont_deaf_iph = +(if_any(starts_with("DGNS_CD"),  ~. %in% deafness_icds)), # need to combine these with dems
           cohort_exclusion_cont_blind_iph = +(if_any(starts_with("DGNS_CD"),  ~. %in% blindness_icds)), # need to combine these with dems
           cohort_exclusion_cont_pall_iph = +(if_any(starts_with("DGNS_CD"),  ~. %in% "Z515")), # palliative care encounter
           cohort_exclusion_cont_cancer_iph = +(if_any(contains("DGNS_CD"),  ~. %in% cancer_icds)) 
    ) |>
    select(BENE_ID, starts_with("cohort"))

# keep only one row per beneficiary -- do this by summarizing by BENE_ID and then capping sum at 1
icds_adj_cont_clean <-
    icds_adj_cont |>
    select(BENE_ID, starts_with("cohort")) |>
    group_by(BENE_ID) |>
    summarize(across(starts_with("cohort"), ~ifelse(sum(.x) >= 1, 1, 0)))  |>
    ungroup()

# keep only one row per beneficiary -- do this by summarizing by BENE_ID and then capping sum at 1
icds_adj_cal_clean <-
    icds_adj_cal |>
    select(BENE_ID, starts_with("cohort")) |>
    group_by(BENE_ID) |>
    summarize(across(starts_with("cohort"), ~ifelse(sum(.x) >= 1, 1, 0)))  |>
    ungroup()


# keep only one row per beneficiary -- do this by summarizing by BENE_ID and then capping sum at 1
icds_adj_12mos_cal_clean <-
    icds_adj_12mos_cal |>
    select(BENE_ID, starts_with("cohort")) |>
    group_by(BENE_ID) |>
    summarize(across(starts_with("cohort"), ~ifelse(sum(.x) >= 1, 1, 0)))  |>
    ungroup() 

# save output files
saveRDS(icds_adj_cont_clean, "data/tafiph/iph_cohort_exclusions_cont.rds") # save eligibiiltiy criteria to merge with baseline file, next step is to remove those who have exclusion criteria via other services files
saveRDS(icds_adj_cal_clean, "data/tafiph/iph_cohort_exclusions_cal.rds") # save eligibiiltiy criteria to merge with baseline file, next step is to remove those who have exclusion criteria via other services files
saveRDS(icds_adj_12mos_cal_clean, "data/tafiph/iph_cohort_exclusions_12mos_cal.rds") # save eligibiiltiy criteria to merge with baseline file, next step is to remove those who have exclusion criteria via other services files

    
    
