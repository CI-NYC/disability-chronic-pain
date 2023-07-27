################################################################################
################################################################################
###  MERGE COHORT EXCLUSION
###  Kat Hoffman, March 2023
###  Purpose: merge all the data sets containing cohort exclusion indicator variables
###  Output: one data set with all cohort exclusion indicator variables
################################################################################
################################################################################

# Set up -----------------------------------------------------------------------


# load libraries
library(arrow)
library(tidyverse)
library(tidylog)
library(lubridate)
library(data.table)
library(janitor)

dts_cohorts <- open_dataset("data/tafdedts/dts_cohorts.parquet") |>
    collect() 

td <- "data/"
files <- list.files(td, pattern = "cohort_exclusion*", recursive = T)
files[[1]] <- "tafdebse/cohort_exclusion_age.rds" 
files[[2]] <- "tafdebse/cohort_exclusion_12mos_cal_dual.rds"
cohort_exclusion_list <- map(files, ~read_rds(paste0(td, .x))) # read in all cohort exclusions

cohort_exclusion_list[[1]]

# cohort_exclusion_list |>
#     map(function(x) {
#         x |> get_dupes(BENE_ID)
#     })

# Reduce all data frames
cohort_exclusion_df <- 
    reduce(cohort_exclusion_list, ~left_join(.x,.y)) |>
    mutate(across(where(is.numeric), ~replace_na(.x, 0))) 

names(cohort_exclusion_df)

cohort_exclusion_df_tmp <-
    cohort_exclusion_df |>
    mutate(
        cohort_exclusion_deaf = case_when(cohort_exclusion_deaf == 1 ~ 1,
                                          cohort_exclusion_cont_deaf_iph == 1 ~ 1, # just picking the furthest out deaf/blind values
                                          cohort_exclusion_cont_deaf_oth == 1 ~ 1,
                                          TRUE ~ 0),
        cohort_exclusion_blind = case_when(cohort_exclusion_blind == 1 ~ 1,
                                           cohort_exclusion_cont_blind_iph == 1 ~ 1, # just picking the furthest out deaf/blind values
                                           cohort_exclusion_cont_blind_oth == 1 ~ 1,
                                           TRUE ~ 0),
        cohort_exclusion_cal_itlcdsbl =  case_when(cohort_exclusion_cal_itlcdsbl_iph == 1 ~ 1,
                                                    cohort_exclusion_cal_itlcdsbl == 1 ~ 1,
                                                    TRUE ~ 0),
        cohort_exclusion_cal_dementia =  case_when(cohort_exclusion_cal_dementia_iph == 1 ~ 1,
                                                   cohort_exclusion_cal_dementia == 1 ~ 1,
                                                   TRUE ~ 0),
        cohort_exclusion_cal_schiz = case_when(cohort_exclusion_cal_schiz_iph == 1 ~ 1,
                                               cohort_exclusion_cal_schiz == 1 ~ 1,
                                               TRUE ~ 0),
        cohort_exclusion_cal_speech =  case_when(cohort_exclusion_cal_speech_iph == 1 ~ 1,
                                                 cohort_exclusion_cal_speech == 1 ~ 1,
                                                 TRUE ~ 0),
        cohort_exclusion_cal_cerpals =  case_when(cohort_exclusion_cal_cerpals_iph == 1 ~ 1,
                                                  cohort_exclusion_cal_cerpals == 1 ~ 1,
                                                  TRUE ~ 0),
        cohort_exclusion_cal_epilepsy =  case_when(cohort_exclusion_cal_epilepsy_iph == 1 ~ 1,
                                                   cohort_exclusion_cal_epilepsy == 1 ~ 1,
                                                   TRUE ~ 0),
        cohort_exclusion_cal_pall =  case_when(cohort_exclusion_cal_pall_iph == 1 ~ 1,
                                               cohort_exclusion_cal_pall == 1 ~ 1,
                                               TRUE ~ 0),
        cohort_exclusion_cal_cancer =  case_when(cohort_exclusion_cal_cancer_iph == 1 ~ 1,
                                                 cohort_exclusion_cal_cancer == 1 ~ 1,
                                                 cohort_exclusion_cal_cancer_elig == 1 ~ 1,
                                                 TRUE ~ 0),
        cohort_exclusion_cal_dual = case_when(cohort_exclusion_cal_dual == 1 ~ 1,
                                               cohort_exclusion_cal_dual_elig == 1 ~ 1,
                                               TRUE ~ 0))


cohort_exclusion_df_tmp2 <-
    cohort_exclusion_df_tmp |>
    mutate(
        cohort_exclusion_deaf = case_when(cohort_exclusion_deaf == 1 ~ 1,
                                          cohort_exclusion_cont_deaf_iph == 1 ~ 1, # just picking the furthest out deaf/blind values
                                          cohort_exclusion_cont_deaf_oth == 1 ~ 1,
                                          TRUE ~ 0),
        cohort_exclusion_blind = case_when(cohort_exclusion_blind == 1 ~ 1,
                                           cohort_exclusion_cont_blind_iph == 1 ~ 1, # just picking the furthest out deaf/blind values
                                           cohort_exclusion_cont_blind_oth == 1 ~ 1,
                                           TRUE ~ 0),
        cohort_exclusion_12mos_cal_itlcdsbl =  case_when(cohort_exclusion_12mos_cal_itlcdsbl_iph == 1 ~ 1,
                                                   cohort_exclusion_12mos_cal_itlcdsbl == 1 ~ 1,
                                                   TRUE ~ 0),
        cohort_exclusion_12mos_cal_dementia =  case_when(cohort_exclusion_12mos_cal_dementia_iph == 1 ~ 1,
                                                   cohort_exclusion_12mos_cal_dementia == 1 ~ 1,
                                                   TRUE ~ 0),
        cohort_exclusion_12mos_cal_schiz = case_when(cohort_exclusion_12mos_cal_schiz_iph == 1 ~ 1,
                                               cohort_exclusion_12mos_cal_schiz == 1 ~ 1,
                                               TRUE ~ 0),
        cohort_exclusion_12mos_al_speech =  case_when(cohort_exclusion_12mos_cal_speech_iph == 1 ~ 1,
                                                 cohort_exclusion_12mos_cal_speech == 1 ~ 1,
                                                 TRUE ~ 0),
        cohort_exclusion_12mos_cal_cerpals =  case_when(cohort_exclusion_12mos_cal_cerpals_iph == 1 ~ 1,
                                                  cohort_exclusion_12mos_cal_cerpals == 1 ~ 1,
                                                  TRUE ~ 0),
        cohort_exclusion_12mos_cal_epilepsy =  case_when(cohort_exclusion_12mos_cal_epilepsy_iph == 1 ~ 1,
                                                   cohort_exclusion_12mos_cal_epilepsy == 1 ~ 1,
                                                   TRUE ~ 0),
        cohort_exclusion_12mos_cal_pall =  case_when(cohort_exclusion_12mos_cal_pall_iph == 1 ~ 1,
                                               cohort_exclusion_12mos_cal_pall == 1 ~ 1,
                                               TRUE ~ 0),
        cohort_exclusion_12mos_cal_cancer =  case_when(cohort_exclusion_12mos_cal_cancer_iph == 1 ~ 1,
                                                 cohort_exclusion_12mos_cal_cancer == 1 ~ 1,
                                                 cohort_exclusion_12mos_cal_cancer_elig == 1 ~ 1,
                                                 TRUE ~ 0),
        cohort_exclusion_12mos_cal_dual = case_when(cohort_exclusion_12mos_cal_dual == 1 ~ 1,
                                              cohort_exclusion_12mos_cal_dual_elig == 1 ~ 1,
                                              TRUE ~ 0))

# split up into two cleaning for time/copy-paste issues on AWS
cohort_exclusion_df_clean <- cohort_exclusion_df_tmp |>
    mutate(
        cohort_exclusion_cont_itlcdsbl =  case_when(cohort_exclusion_cont_itlcdsbl_iph == 1 ~ 1,
                                                   cohort_exclusion_cont_itlcdsbl == 1 ~ 1,
                                                   TRUE ~ 0),
        cohort_exclusion_cont_dementia =  case_when(cohort_exclusion_cont_dementia_iph == 1 ~ 1,
                                                   cohort_exclusion_cont_dementia == 1 ~ 1,
                                                   TRUE ~ 0),
        cohort_exclusion_cont_schiz = case_when(cohort_exclusion_cont_schiz_iph == 1 ~ 1,
                                               cohort_exclusion_cont_schiz == 1 ~ 1,
                                               TRUE ~ 0),
        cohort_exclusion_cont_speech =  case_when(cohort_exclusion_cont_speech_iph == 1 ~ 1,
                                                 cohort_exclusion_cont_speech == 1 ~ 1,
                                                 TRUE ~ 0),
        cohort_exclusion_cont_cerpals =  case_when(cohort_exclusion_cont_cerpals_iph == 1 ~ 1,
                                                  cohort_exclusion_cont_cerpals == 1 ~ 1,
                                                  TRUE ~ 0),
        cohort_exclusion_cont_epilepsy =  case_when(cohort_exclusion_cont_epilepsy_iph == 1 ~ 1,
                                                   cohort_exclusion_cont_epilepsy == 1 ~ 1,
                                                   TRUE ~ 0),
        cohort_exclusion_cont_pall =  case_when(cohort_exclusion_cont_pall_iph == 1 ~ 1,
                                               cohort_exclusion_cont_pall == 1 ~ 1,
                                               TRUE ~ 0),
        cohort_exclusion_cont_cancer =  case_when(cohort_exclusion_cont_cancer_iph == 1 ~ 1,
                                                 cohort_exclusion_cont_cancer == 1 ~ 1,
                                                 cohort_exclusion_cont_cancer_elig == 1 ~ 1,
                                                 TRUE ~ 0),
        cohort_exclusion_cont_dual = case_when(cohort_exclusion_cont_dual == 1 ~ 1,
                                               cohort_exclusion_cont_dual_elig == 1 ~ 1,
                                               TRUE ~ 0)
        
           ) |>
    select(-contains("iph"),
           -contains("oth"),
           -contains("cancer_elig"),
           -contains("dual_elig")) # remove excess columns
   


cohort <-
    dts_cohorts |>
    left_join(cohort_exclusion_df_clean) |>
    select(BENE_ID,
           contains("dt"), 
           study_completed_cont = study_completed, # clarify that this is study completion for the continuous enrollment cohort
           cohort_exclusion_age,
           cohort_exclusion_sex,
           cohort_exclusion_state, 
           cohort_exclusion_deaf,
           cohort_exclusion_blind,
           contains("cohort_exclusion_cal"),
           contains("cohort_exclusion_12mos_cal"),
           contains("cohort_exclusion_cont"),
           everything()
    ) 
names(cohort)

# check on missing values
tmp <- cohort |> select(contains("cohort_exclusion"))
colSums(is.na(cohort))

write_rds(cohort, "data/final/cohort_eligibility.rds")

