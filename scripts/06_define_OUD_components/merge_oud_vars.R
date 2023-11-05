################################################################################
################################################################################
###  MERGE ALL OUD RELATED VARIABLES
###  Kat Hoffman, March 2023
###  Purpose: clean TAFOTH and TAFIPH files for antidepressant NDC codes
###  Output: cleaned data file containing minimum date the beneficiary ("data/final/antidepressant_rx.rds")
###        has a adhd ICD code in the study duration
###         and indicators of whether it occurs in washout or overall study duration
################################################################################
################################################################################

library(tidyverse)
library(arrow)

dts_cohorts <- open_dataset("projects/create_cohort/data/tafdedts/dts_cohorts.parquet") |>
    collect() 

# all oud related data frames
oud_hillary <- read_rds("projects/create_cohort/data/final/oud_hillary.rds")
oud_poison <- read_rds("projects/create_cohort/data/final/oud_poison.rds")
oud_abuse <- read_rds("projects/create_cohort/data/final/oud_abuse.rds")
oud_misuse <- read_rds("projects/create_cohort/data/final/oud_misuse.rds")
moud_bup <- read_rds("projects/create_cohort/data/oud_info/bup/all_bup_intervals.rds")
moud_nal <- read_rds("projects/create_cohort/data/oud_info/nal/all_nal_intervals.rds")
moud_met <- read_rds("projects/create_cohort/data/oud_info/met/all_met_intervals.rds")

ouds <- dts_cohorts |> # to get full cohort id's to left join into
    select(BENE_ID) |>
    left_join(oud_abuse) |>
    left_join(oud_hillary) |>
    left_join(oud_poison) |>
    left_join(moud_bup) |>
    left_join(moud_nal) |>
    left_join(moud_met) |>
    left_join(oud_misuse) |>
    # Create merged OUD variables
    mutate(
        # Any OUD component during calendar year enrollment
        cohort_exclusion_oud_cal =
            cohort_exclusion_oud_misuse + # any misuse during washout
            oud_moud_nal_washout_cal + # any of the MOUDs during washout
            oud_moud_met_washout_cal +
            oud_moud_bup_washout_cal +
            oud_hillary_washout_cal + # any DSM diagnosis codes during washout
            oud_poison_washout_cal, # any poison diagnosis codes during washout
        # Any OUD component during calendar year enrollment
        cohort_exclusion_oud_12mos_cal =
            cohort_exclusion_oud_misuse + # any misuse during washout
            oud_moud_nal_washout_12mos_cal + # any of the MOUDs during washout
            oud_moud_met_washout_12mos_cal +
            oud_moud_bup_washout_12mos_cal +
            oud_hillary_washout_12mos_cal + # any DSM diagnosis codes during washout
            oud_poison_washout_12mos_cal, # any poison diagnosis codes during washout
        # Any OUD component during continuous enrollment washout
        cohort_exclusion_oud_cont =
            cohort_exclusion_oud_misuse + # any misuse during washout
            oud_moud_nal_washout_cont + # any of the MOUDs during washout
            oud_moud_met_washout_cont +
            oud_moud_bup_washout_cont +
            oud_hillary_washout_cont + # any DSM diagnosis  codes during washout
            oud_poison_washout_cont, # any poison dg codes during washout
        # Any OUD Misuse, Poison, or Abuse during cont study duration
        oud_misuse_abuse_poison_cont = 
            oud_misuse_cont_any +
            oud_poison_study_cont +
            oud_hillary_study_cont,
        # Any MOUD during cont study duration
        any_moud_cont = oud_moud_nal_study_cont +
            oud_moud_met_study_cont +
            oud_moud_bup_study_cont,
        
        any_moud_12mos_cal = oud_moud_nal_washout_12mos_cal +
            oud_moud_met_washout_12mos_cal +
            oud_moud_bup_washout_12mos_cal,
        # cap at 1
        any_moud_cont = ifelse(any_moud_cont >= 1, 1, 0),

        # Any OUD component during cont study duration
        oud_cont = 
            oud_misuse_cont_any +
            oud_poison_study_cont +
            oud_hillary_study_cont +
            any_moud_cont,
        # if any are > 1, cap at 1 (indicator variable)
        oud_misuse_abuse_poison_cont = ifelse(oud_misuse_abuse_poison_cont >= 1, 1, 0),
        oud_cont = ifelse(oud_cont >= 1, 1, 0),
        
        # Any MOUD during calendar study duration
        any_moud_cal = oud_moud_nal_study_cal +
            oud_moud_met_study_cal +
            oud_moud_bup_study_cal,
        # cap at 1
        any_moud_cal = ifelse(any_moud_cal >= 1, 1, 0),
        
        # Any OUD Misuse, Poison, or Abuse during cal study duration
        oud_misuse_abuse_poison_cal = 
            oud_misuse_cal_any +
            oud_poison_study_cal +
            oud_hillary_study_cal,
        # Any OUD component during calendar study duration
        oud_cal = 
            oud_misuse_cal_any +
            oud_poison_study_cal +
            oud_hillary_study_cal +
            any_moud_cal,
        # if any are > 1, cap at 1 (indicator variable)
        oud_misuse_abuse_poison_cal = ifelse(oud_misuse_abuse_poison_cal >= 1, 1, 0),
        oud_cal = ifelse(oud_cal >= 1, 1, 0)
    )


saveRDS(ouds, "projects/create_cohort/data/final/all_ouds.rds")

