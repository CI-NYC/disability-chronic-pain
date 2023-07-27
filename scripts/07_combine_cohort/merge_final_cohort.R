################################################################################
################################################################################
###  MERGE FINAL COHORT FILES
###  Kat Hoffman, March 2023
###  Purpose: merge all the data sets and create the big descriptive cohorts and smaller analytical cohrot
###  Output: data frames in data/final containing wide data set for desc and analysis cohorts
################################################################################
################################################################################


# set up ------------------------------------------------------------------

library(tidylog)
library(tidyverse)
library(janitor)
library(arrow)
library(gtsummary)
library(gt)
library(tictoc)

# mental illness data frames
anxiety <- read_rds("data/final/anxiety.rds")
depression <- read_rds("data/final/depression.rds")
adhd <- read_rds("data/final/adhd.rds")
mental_ill <- read_rds("data/final/mental_ill.rds")
bipolar <- read_rds("data/final/bipolar.rds")

# prescription data frames
antidepressants <- read_rds("data/final/antidepressants.rds")
benzodiazepines <- read_rds("data/final/benzodiazepines.rds")
antipsychotics <- read_rds("data/final/antipsychotics.rds")
stimulants <- read_rds("data/final/stimulants.rds")
opioid_pain_rxs <- read_rds("data/final/opioid_pains.rds")
moodstabilizers <- read_rds("data/final/moodstabilizers.rds")

# merge all mental illnesses
mental_illnesses <- reduce(list(anxiety, depression, adhd, mental_ill, bipolar),
                           ~full_join(.x, .y))

# merge all prescriptions
prescriptions <- reduce(list(antidepressants, 
                             benzodiazepines, 
                             antipsychotics,
                             stimulants,
                             opioid_pain_rxs, 
                             moodstabilizers),
                        ~full_join(.x, .y))

# save data frames to read in faster later
saveRDS(mental_illnesses, "data/final/all_mental_illnesses.rds")
saveRDS(prescriptions, "data/final/all_prescriptions.rds")

cohort <- read_rds("data/final/cohort_eligibility.rds")
dem_df <- open_dataset("data/tafdebse/dem_df.parquet") |> collect() # kat, should move this to final folder
death <- read_rds("data/final/death_dts.rds")
pain <- read_rds("data/final/pain.rds")
chronic_pain <- read_rds("data/final/chronic_pain_wide.rds")
chronic_pain_12mos <- read_rds("data/final/chronic_pain_wide_12mos.rds") 
ouds <- read_rds("data/final/all_ouds.rds")
censoring <- read_rds("data/final/censoring_full.rds")

mental_illnesses <- read_rds("data/final/all_mental_illnesses.rds")
prescriptions <- read_rds("data/final/all_prescriptions.rds")

joined_df <-
    cohort |>
    left_join(dem_df) |>
    left_join(chronic_pain) |>
    left_join(chronic_pain_12mos) |>
    left_join(pain) |>
    mutate(across(where(is.numeric), ~replace_na(.x, 0)))  |>
    left_join(mental_illnesses) |>
    left_join(prescriptions) |>
    left_join(ouds) |>
    left_join(death) |>
    mutate(death_ever = ifelse(is.na(death_ever), 0, death_ever)) |>
    left_join(censoring)
    
saveRDS(joined_df, "data/final/joined_df.rds") # pre data cleaning

joined_df <- read_rds("data/final/joined_df.rds")

##### DATA CLEANING

joined_df_tmp_exp <- 
    joined_df |>
    mutate(
        disability_pain_cal = case_when(disability_washout_cal == 1 & chronic_pain_any_month_0 == 1 ~ "disability and chronic pain",
                                    disability_washout_cal == 1 & chronic_pain_any_month_0 == 0 ~ "disability only",
                                   disability_washout_cal == 0 & chronic_pain_any_month_0 == 1 ~ "chronic pain only",
                                    disability_washout_cal == 0 & chronic_pain_any_month_0 == 0 ~ "neither"),
disability_pain_12mos_cal = case_when(disability_washout_12mos_cal == 1 & chronic_pain_12mos_any_month_0 == 1 ~ "disability and chronic pain",
                                disability_washout_12mos_cal == 1 & chronic_pain_12mos_any_month_0 == 0 ~ "disability only",
                                disability_washout_12mos_cal == 0 & chronic_pain_12mos_any_month_0 == 1 ~ "chronic pain only",
                                disability_washout_12mos_cal == 0 & chronic_pain_12mos_any_month_0 == 0 ~ "neither"),
disability_pain_cont = case_when(disability_washout_cont == 1 & chronic_pain_any_month_0 == 1 ~ "disability and chronic pain",
                                      disability_washout_cont == 1 & chronic_pain_any_month_0 == 0 ~ "disability only",
                                      disability_washout_cont == 0 & chronic_pain_any_month_0 == 1 ~ "chronic pain only",
                                      disability_washout_cont == 0 & chronic_pain_any_month_0 == 0 ~ "neither"),
disability_pain_nomin_cal = case_when(disability_washout_cal == 1 & pain_washout_cal == 1 ~ "disability and chronic pain",
                                disability_washout_cal == 1 & pain_washout_cal == 0 ~ "disability only",
                                disability_washout_cal == 0 & pain_washout_cal == 1 ~ "chronic pain only",
                                disability_washout_cal == 0 & pain_washout_cal == 0 ~ "neither"),
    across(starts_with("disability_pain"), 
       ~fct_relevel(.x, "disability and chronic pain","disability only","chronic pain only")))
      
joined_df_tmp <-
    joined_df_tmp_exp |>
        mutate(
            chronic_pain_ever_cal = case_when(chronic_pain_n_months >= 1 ~ 1, TRUE ~ 0),
            chronic_pain_12mos_ever_cal = case_when(chronic_pain_12mos_n_months >= 1 ~ 1, TRUE ~ 0),
            dem_probable_high_income = case_when(probable_high_income_cont == 1 ~ 1,
                                                 as.numeric(INCM_CD) >= 3 ~ 1, # https://resdac.org/cms-data/variables/income-relative-federal-poverty-level-latest-year
                                                TRUE ~ 0),
         dem_race = case_when(RACE_ETHNCTY_CD == "1" ~ "White, non-Hispanic",
                          RACE_ETHNCTY_CD == "2" ~ "Black, non-Hispanic",
                          RACE_ETHNCTY_CD == "3" ~ "Asian, non-Hispanic",
                          RACE_ETHNCTY_CD == "4" ~ "American Indian and Alaska Native (AIAN), non-Hispanic",
                          RACE_ETHNCTY_CD == "5" ~ "Hawaiian/Pacific Islander",
                          RACE_ETHNCTY_CD == "6" ~ "Multiracial, non-Hispanic",
                          RACE_ETHNCTY_CD == "7" ~ "Hispanic, all races"
                          ),
         # condensed race category for analysis
         dem_race_cond = case_when(dem_race == "American Indian and Alaska Native (AIAN), non-Hispanic" ~ "AIAN_or_HPI",
                              dem_race == "Hawaiian/Pacific Islander" ~ "AIAN_or_HPI",
                              dem_race == "Multiracial, non-Hispanic" ~ "multi_or_na",
                              is.na(dem_race) ~ "multi_or_na",
                              TRUE ~ dem_race
         ),
         dem_primary_language_english = case_when(
             PRMRY_LANG_GRP_CD == "E" ~ 1,
             PRMRY_LANG_GRP_CD != "E" ~ 0
         ),
         dem_married_or_partnered = case_when(as.numeric(MRTL_STUS_CD) <= 8 ~ 1,
                                              as.numeric(MRTL_STUS_CD) >= 8 ~ 0),
         dem_household_size = case_when(HSEHLD_SIZE_CD == "01" ~ "1",
                                        HSEHLD_SIZE_CD == "02" ~ "2",
                                        as.numeric(HSEHLD_SIZE_CD) > 2 ~ "2+"
                                        ), # note this is capped at 8 people - may want to recategorize
         dem_veteran = as.numeric(VET_IND),
         dem_tanf_benefits = case_when(TANF_CASH_CD == "2" ~ 1,
                                       TANF_CASH_CD == "1" ~ 0,
                                       TANF_CASH_CD == "0" ~ 0
                                       ),
         dem_ssi_benefits = case_when(SSI_STATE_SPLMT_CD == "000" ~ "Not Applicable",
                                      SSI_STATE_SPLMT_CD %in% c("001","002") ~ "Mandatory or optional"),
         dem_sex = SEX_CD,
         dem_age = age_enrollment,
         fips = paste0(BENE_STATE_CD, BENE_CNTY_CD)
         ) 
    


# sum all the cohort exclusions relevant to the continuous enrollment cohort
joined_df_clean <-
    joined_df_tmp |>
    mutate(cohort_exclusion_cont =
               cohort_exclusion_age +
               cohort_exclusion_sex +
               cohort_exclusion_state +
               cohort_exclusion_deaf +
               cohort_exclusion_blind +
               cohort_exclusion_cont_deafblind +
               cohort_exclusion_cont_no_washout + # no end of washout period
               cohort_exclusion_cont_no_elig + # no eligibility criteria
               cohort_exclusion_cont_unknown_disability + # unsure whether they have a disability from eligibility code
               cohort_exclusion_cont_dual + # dual eligible
               cohort_exclusion_cont_pregnancy +
               cohort_exclusion_cont_ltc +
               cohort_exclusion_cont_itlcdsbl +
               cohort_exclusion_cont_dementia + 
               cohort_exclusion_cont_schiz + 
               cohort_exclusion_cont_speech + 
               cohort_exclusion_cont_cerpals + 
               cohort_exclusion_cont_epilepsy + 
               cohort_exclusion_cont_pall + 
               cohort_exclusion_cont_cancer +
               cohort_exclusion_cont_institution + # hospice, institution, LTC?
               cohort_exclusion_cont_tb,  # tuberculosis
           # note we aren't including OUD exclusion criteria for the continuous cohort

    cohort_exclusion_cal =
        cohort_exclusion_cal_jan2016 +
        cohort_exclusion_age +
        cohort_exclusion_sex +
        cohort_exclusion_state +
        cohort_exclusion_deaf +
        cohort_exclusion_blind +
        cohort_exclusion_cal_deafblind +
        cohort_exclusion_cal_no_washout + # no end of washout period
        cohort_exclusion_cal_no_elig + # no eligibility criteria
        cohort_exclusion_cal_unknown_disability + # unsure whether they have a disability from eligibility code
        cohort_exclusion_cal_dual + # dual eligible
        cohort_exclusion_cal_pregnancy +
        cohort_exclusion_cal_ltc +
        cohort_exclusion_cal_itlcdsbl +
        cohort_exclusion_cal_dementia + 
        cohort_exclusion_cal_schiz + 
        cohort_exclusion_cal_speech + 
        cohort_exclusion_cal_cerpals + 
        cohort_exclusion_cal_epilepsy + 
        cohort_exclusion_cal_pall + 
        cohort_exclusion_cal_cancer +
        cohort_exclusion_cal_institution + # hospice, institution, LTC?
        cohort_exclusion_cal_tb +  # tuberculosis
        # Add OUD variables for this cohort
        cohort_exclusion_oud_cal,
    
    cohort_exclusion_12mos_cal =
        cohort_exclusion_cal_jan2016 +
        cohort_exclusion_age +
        cohort_exclusion_sex +
        cohort_exclusion_state +
        cohort_exclusion_deaf +
        cohort_exclusion_blind +
        cohort_exclusion_cal_pregnancy +
        # Add OUD variables for this cohort
    cohort_exclusion_12mos_cal_no_washout +
    cohort_exclusion_12mos_cal_institution + 
    cohort_exclusion_12mos_cal_unknown_disability +
    cohort_exclusion_12mos_cal_not_elig +
    cohort_exclusion_12mos_cal_deafblind +
    cohort_exclusion_12mos_cal_tb +
    cohort_exclusion_12mos_cal_no_elig +
    cohort_exclusion_12mos_cal_dual +
    cohort_exclusion_12mos_cal_ltc +
    cohort_exclusion_12mos_cal_itlcdsbl +
    cohort_exclusion_12mos_cal_dementia +
    cohort_exclusion_12mos_cal_schiz +
    cohort_exclusion_12mos_cal_speech +
    cohort_exclusion_12mos_cal_cerpals +
    cohort_exclusion_12mos_cal_epilepsy +
    cohort_exclusion_12mos_cal_pall +
    cohort_exclusion_12mos_cal_cancer +
        cohort_exclusion_oud_12mos_cal
)

joined_df_clean |>
    select(contains("cohort_exclusion")) |>
    saveRDS("output/cohort_exclusions_df.rds")

# filter out anyone with exclusion criteria of interest (note, no OUD)
desc_cohort <- joined_df_clean |>
    filter(cohort_exclusion_cont == 0)

# filter out anyone with analytic exclusion criteria of interest (calendar year + oud)
analysis_cohort <- joined_df_clean |>
    filter(cohort_exclusion_cal == 0) |>
    mutate(disability_pain_cal = fct_relevel(disability_pain_cal, "disability and chronic pain", "chronic pain only"),
           disability_pain_nomin_cal = fct_relevel(disability_pain_nomin_cal, "disability and chronic pain", "chronic pain only"))

# filter out anyone with analytic exclusion criteria of interest (calendar year + oud)
sens_12mos_analysis_cohort <- joined_df_clean |>
    filter(cohort_exclusion_12mos_cal == 0) |>
    mutate(disability_pain_12mos_cal = fct_relevel(disability_pain_12mos_cal, "disability and chronic pain", "chronic pain only"))

exposures_tmp <- joined_df_clean |>
    select(BENE_ID, starts_with("disability_pain"))

saveRDS(exposures_tmp, "data/final/exposures_tmp.rds")

analysis_cohort |> count(disability_pain_cal)
analysis_cohort |> count(disability_pain_nomin_cal)
sens_12mos_analysis_cohort |> count(disability_pain_12mos_cal)

# n's for each cohort
desc_cohort |> count(disability_pain_cont)
analysis_cohort |> count(disability_pain_cal)

write_rds(desc_cohort, "data/final/desc_cohort.rds")
write_rds(analysis_cohort, "data/final/analysis_cohort.rds")
write_rds(sens_12mos_analysis_cohort, "data/final/sens_12mos_analysis_cohort.rds")

sens_12mos_analysis_cohort <- read_rds("data/final/sens_12mos_analysis_cohort.rds")

analysis_cohort <- read_rds("data/final/analysis_cohort.rds")

check_extra_n <- function(max_month){
    
    months_to_check <- paste0("chronic_pain_any_month_", 1:max_month)
    
    dis <-
        analysis_cohort |>
        select(disability_washout_cal,
               chronic_pain_any_month_0,
               one_of(months_to_check)) |>
        filter(disability_washout_cal == 1,
               chronic_pain_any_month_0 == 0)
    
    n_add_dis <-
        dis |>
        select( one_of(months_to_check)) |>
        filter_at(vars(months_to_check), any_vars(. == 1)) |>
        nrow()
    
    freq_add_dis <- n_add_dis / nrow(dis)
    
    neither <-
        analysis_cohort |>
        select(disability_washout_cal,
               chronic_pain_any_month_0,
               one_of(months_to_check)) |>
        filter(disability_washout_cal == 0,
               chronic_pain_any_month_0 == 0)
    
    n_add_neither <-
        neither |>
        select( one_of(months_to_check)) |>
        filter_at(vars(months_to_check), any_vars(. == 1)) |>
        nrow()
    
    freq_add_neither <- n_add_neither / nrow(neither)
    
    return(data.frame(rolling_window_start = max_month, n_add_dis = n_add_dis, freq_add_dis = freq_add_dis,
                      n_add_neither = n_add_neither,  freq_add_neither = freq_add_neither))
}

map_dfr(1:6, check_extra_n)


check_extra_n_12mos <- function(max_month){
    
    months_to_check <- paste0("chronic_pain_12mos_any_month_", 1:max_month)
    
    dis <-
        analysis_cohort |>
        select(disability_washout_cal,
               chronic_pain_12mos_any_month_0,
               one_of(months_to_check)) |>
        filter(disability_washout_cal == 1,
               chronic_pain_12mos_any_month_0 == 0)
    
    n_add_dis <-
        dis |>
        select( one_of(months_to_check)) |>
        filter_at(vars(months_to_check), any_vars(. == 1)) |>
        nrow()
    
    freq_add_dis <- n_add_dis / nrow(dis)
    
    neither <-
        analysis_cohort |>
        select(disability_washout_cal,
               chronic_pain_12mos_any_month_0,
               one_of(months_to_check)) |>
        filter(disability_washout_cal == 0,
               chronic_pain_12mos_any_month_0 == 0)
    
    n_add_neither <-
        neither |>
        select( one_of(months_to_check)) |>
        filter_at(vars(months_to_check), any_vars(. == 1)) |>
        nrow()
    
    freq_add_neither <- n_add_neither / nrow(neither)
    
    return(data.frame(rolling_window_start = max_month, n_add_dis = n_add_dis, freq_add_dis = freq_add_dis,
                      n_add_neither = n_add_neither,  freq_add_neither = freq_add_neither))
}


map_dfr(1:6, check_extra_n_12mos)


### Table of how many people would have disbaility and pain out of disbality only if we stretched out our chronic pain defs

### Disability only + neither -- who gains chronic pain as you stretch out the itnervals

### make the same table for 1 year out
