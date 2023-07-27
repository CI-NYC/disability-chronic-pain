################################################################################
################################################################################
###  CREATE DESCRIPTIVE TABLES FOR VARIOUS COHORT SUBSETS
###  Kat Hoffman, March 2023
###  Output: saved gt / cvs table objects for descriptive, analysis, and age restricted analysis cohort
################################################################################
################################################################################

library(tidylog)
library(tidyverse)
library(janitor)
library(arrow)
library(gtsummary)
library(gt)
library(tictoc)

desc_cohort <-  read_rds("data/final/desc_cohort.rds") 
analysis_cohort <-  read_rds("data/final/analysis_cohort.rds") 

dat_lmtp <- read_rds("data/final/dat_lmtp.rds") 

dat_subset <- dat_lmtp |>
    select(BENE_ID) |>
    left_join(analysis_cohort)

# Create data for table 1 pieces -----------------------------------------------

dem_tbl_data <- desc_cohort  |>
    select(disability_pain_cont, 
           dem_age,
           dem_sex,
           dem_race,
           dem_primary_language_english,
           dem_married_or_partnered,
           dem_probable_high_income,
           starts_with("dem_"),
           -dem_race_cond
           ) |>
    labelled::set_variable_labels(
        dem_age = "Age",
        dem_sex = "Sex",
        dem_race = "Race/Ethnicity",
        dem_primary_language_english = "Primary Language English",
        dem_married_or_partnered = "Married/Partnered",
        dem_probable_high_income = "High Income",
        dem_household_size = "Household size",
        dem_veteran = "Veteran",
        dem_tanf_benefits = "TANF Benefits",
        dem_ssi_benefits = "SSI Benefits"
    ) 


comorb_tbl_data <- desc_cohort  |>
    select(disability_pain_cont, 
          # chronic_pain_study_cont,
           bipolar_study_cont,
           anxiety_study_cont,
           adhd_study_cont,
           depression_study_cont,
           mental_ill_study_cont,
           antidepressant_study_cont,
           benzodiazepine_study_cont,
           antipsychotic_study_cont, 
           stimulant_study_cont,
           moodstabilizer_study_cont,
           opioid_pain_study_cont
    ) |>
    labelled::set_variable_labels(
        #chronic_pain_study_cont = "Chronic Pain",
        bipolar_study_cont = "Bipolar",
        anxiety_study_cont = "Anxiety",
        adhd_study_cont = "ADD/ADHD",
        depression_study_cont = "Depression",
        mental_ill_study_cont = "Other Mental Illness",
        antidepressant_study_cont = "Antidepressant Prescription",
        benzodiazepine_study_cont = "Benzodiazepine Prescription",
        antipsychotic_study_cont = "Anti-psychotic Prescription", 
        stimulant_study_cont = "Stimulant Prescription",
        moodstabilizer_study_cont = "Mood Stabilizer Prescription",
        opioid_pain_study_cont = "Opioid Pain Prescription",
    )


oud_tbl_data <- desc_cohort  |>
    select(disability_pain_cont, 
           oud_abuse_study_cont,
           oud_hillary_study_cal,
           oud_poison_study_cont,
           oud_moud_nal_study_cont,
           oud_moud_met_study_cont,
           oud_moud_bup_study_cont,
           oud_misuse_cont_any,                     
           oud_misuse_cont_avg_distinct_providers,
           oud_misuse_cont_avg_distinct_dispensers,
           oud_misuse_cont_avg_total_days_supply,
           oud_misuse_abuse_poison_cont,
           oud_cont
          
    ) |>
    labelled::set_variable_labels(
        oud_abuse_study_cont = "OUD Diagnosis Abuse",
        oud_hillary_study_cont = "OUD Hillary Abuse",
        
        oud_poison_study_cont = "OUD Diagnosis Poison",
        oud_moud_nal_study_cont  = "MOUD: Nal",
        oud_moud_met_study_cont  = "MOUD: Met",
        oud_moud_bup_study_cont   = "MOUD: Bup",
        oud_misuse_cont_any = "OUD: Probable Misuse",                     
        oud_misuse_cont_avg_distinct_providers = "OUD: Avg. Disinct Providers",
        oud_misuse_cont_avg_distinct_dispensers  = "OUD: Avg. Disinct Dispensers",
        oud_misuse_cont_avg_total_days_supply = "OUD: Avg. Days Supply",
        oud_misuse_abuse_poison_cont = "OUD: Any Abuse, Poison, or Probable Misabuse",
        oud_cont = "OUD: Any"
    )



# Descriptive tables for big cohort ---------------------------------------

##########  DEMOGRPAHICS ################################################## 

tic()
dem_tbl <-
    dem_tbl_data |>
    tbl_summary(by = disability_pain_cont) |>
    bold_labels()
toc()

# write_rds(dem_tbl, "dem_tbl.rds")

latex_tbl <- dem_tbl |>
    as_gt() |>
    as_latex()

write_rds(latex_tbl, "output/dem_tbl_gt.rds")

dem_tbl_df <- dem_tbl |>
    as_gt() |>
    as.data.frame()

dem_tbl_df_select <-
    dem_tbl_df |>
    select(label:stat_4)

write_csv(dem_tbl_df, "tbls/dem_tbl1.csv")


##########  COMORBIDITIES ################################################## 

tic()
comorb_tbl <-
    comorb_tbl_data |>
    tbl_summary(by = disability_pain_cont) 
toc()

latex_tbl <- comorb_tbl |>
    as_gt() |>
    as_latex()

write_rds(latex_tbl, "output/comorb_tbl_gt.rds")


comorb_tbl_df <- comorb_tbl |>
    as_gt() |>
    as.data.frame()

comorb_tbl_df_select <-
    comorb_tbl_df |>
    select(label:stat_4)

write_csv(comorb_tbl_df, "tbls/comorb_tbl1.csv")


##########  OUD  ############################################################ 

tic()
oud_tbl <-
    oud_tbl_data |>
    tbl_summary(by = disability_pain_cont)
toc()


latex_tbl <- oud_tbl |>
    as_gt() |>
    as_latex()

write_rds(latex_tbl, "output/oud_tbl_gt.rds")


oud_tbl_df <- 
    oud_tbl |>
    as_gt() |>
    as.data.frame()

oud_tbl_df_select <-
    oud_tbl_df |>
    select(label:stat_4)

write_csv(oud_tbl_df, "tbls/oud_tbl1.csv")

# Create data for table 2 pieces -----------------------------------------------

dem_tbl2_data <- analysis_cohort  |>
    select(disability_pain_cal, 
           dem_age,
           dem_sex,
           dem_race,
           dem_primary_language_english,
           dem_married_or_partnered,
           dem_probable_high_income,
           starts_with("dem_"),
           -dem_race_cond
    ) |>
    labelled::set_variable_labels(
        dem_age = "Age",
        dem_sex = "Sex",
        dem_race = "Race/Ethnicity",
        dem_primary_language_english = "Primary Language English",
        dem_married_or_partnered = "Married/Partnered",
        dem_probable_high_income = "High Income",
        dem_household_size = "Household size",
        dem_veteran = "Veteran",
        dem_tanf_benefits = "TANF Benefits",
        dem_ssi_benefits = "SSI Benefits"
    )


comorb_tbl2_data <- analysis_cohort  |>
    select(disability_pain_cal, 
           chronic_pain_ever_cal,
           bipolar_study_cal,
           anxiety_study_cal,
           adhd_study_cal,
           depression_study_cal,
           mental_ill_study_cal,
           antidepressant_study_cal,
           benzodiazepine_study_cal,
           antipsychotic_study_cal, 
           stimulant_study_cal,
           moodstabilizer_study_cal,
           opioid_pain_study_cal
    ) |>
    labelled::set_variable_labels(
        chronic_pain_ever_cal  = "Chronic Pain Ever",
        bipolar_study_cal = "Bipolar",
        anxiety_study_cal = "Anxiety",
        adhd_study_cal = "ADD/ADHD",
        depression_study_cal = "Depression",
        mental_ill_study_cal = "Other Mental Illness",
        antidepressant_study_cal = "Antidepressant Prescription",
        benzodiazepine_study_cal = "Benzodiazepine Prescription",
        antipsychotic_study_cal = "Anti-psychotic Prescription", 
        stimulant_study_cal = "Stimulant Prescription",
        moodstabilizer_study_cal = "Mood Stabilizer Prescription",
        opioid_pain_study_cal = "Opioid Pain Prescription",
    )


oud_tbl2_data <- analysis_cohort  |>
    select(disability_pain_cal, 
           oud_abuse_study_cal,
           oud_hillary_study_cal,
           oud_poison_study_cal,
           oud_moud_nal_study_cal,
           oud_moud_met_study_cal,
           oud_moud_bup_study_cal,
           oud_misuse_cal_any,                     
           oud_misuse_cal_avg_distinct_providers,
           oud_misuse_cal_avg_distinct_dispensers,
           oud_misuse_cal_avg_total_days_supply,
           oud_misuse_abuse_poison_cal,
           oud_cal
    ) |>
    labelled::set_variable_labels(
        oud_abuse_study_cal = "OUD Diagnosis Abuse",
        oud_abuse_study_cal = "OUD Diagnosis Hillary",
        oud_poison_study_cal = "OUD Diagnosis Poison",
        oud_moud_nal_study_cal  = "MOUD: Nal",
        oud_moud_met_study_cal  = "MOUD: Met",
        oud_moud_bup_study_cal   = "MOUD: Bup",
        oud_misuse_cal_any = "OUD: Probable Misuse",                     
        oud_misuse_cal_avg_distinct_providers = "OUD: Avg. Disinct Providers",
        oud_misuse_cal_avg_distinct_dispensers  = "OUD: Avg. Disinct Dispensers",
        oud_misuse_cal_avg_total_days_supply = "OUD: Avg. Days Supply",
        oud_misuse_abuse_poison_cal = "OUD: Any Abuse, Poison, or Probable Misabuse",
        oud_cal = "OUD: Any"
    )




##########  DEMOGRPAHICS ################################################## 

tic()
dem_tbl2 <-
    dem_tbl2_data |>
    tbl_summary(by = disability_pain_cal)
toc()

latex_tbl <- dem_tbl2 |>
    as_gt() |>
    as_latex()

write_rds(latex_tbl, "output/dem_tbl2_gt.rds")



dem_tbl2_df <- dem_tbl2 |>
    as_gt() |>
    as.data.frame()

dem_tbl2_df_select <-
    dem_tbl2_df |>
    select(label:stat_4)

write_csv(dem_tbl2_df, "tbls/dem_tbl2.csv")


##########  COMORBIDITIES ################################################## 

tic()
comorb_tbl2 <-
    comorb_tbl2_data |>
    tbl_summary(by = disability_pain_cal) 
toc()


latex_tbl <- comorb_tbl2 |>
    as_gt() |>
    as_latex()

write_rds(latex_tbl, "output/comorb_tbl2_gt.rds")



comorb_tbl2_df <- comorb_tbl2 |>
    as_gt() |>
    as.data.frame()

comorb_tbl2_df_select <-
    comorb_tbl2_df |>
    select(label:stat_4)

write_csv(comorb_tbl2_df, "tbls/comorb_tbl2.csv")


##########  OUD  ##########################################################  

# make table
tic()
oud_tbl2 <-
    oud_tbl2_data |>
    tbl_summary(by = disability_pain_cal) 
toc()

# convert to latex
latex_tbl <- oud_tbl2 |>
    as_gt() |>
    as_latex()

# export to latex code
write_rds(latex_tbl, "output/oud_tbl2_gt.rds")



oud_tbl2_df <- 
    oud_tbl2 |>
    as_gt() |>
    as.data.frame()

oud_tbl2_df_select <-
    oud_tbl2_df |>
    select(label:stat_4)

write_csv(oud_tbl2_df, "tbls/oud_tbl2.csv")






# analytical subset (age) -------------------------------------------------


dem_tbl3_data <- dat_subset  |>
    select(disability_pain_cal, 
           dem_age,
           dem_sex,
           dem_race,
           dem_primary_language_english,
           dem_married_or_partnered,
           dem_probable_high_income,
           starts_with("dem_"),
           -dem_race_cond
    ) |>
    labelled::set_variable_labels(
        dem_age = "Age",
        dem_sex = "Sex",
        dem_race = "Race/Ethnicity",
        dem_primary_language_english = "Primary Language English",
        dem_married_or_partnered = "Married/Partnered",
        dem_probable_high_income = "High Income",
        dem_household_size = "Household size",
        dem_veteran = "Veteran",
        dem_tanf_benefits = "TANF Benefits",
        dem_ssi_benefits = "SSI Benefits"
    )


comorb_tbl3_data <- dat_subset  |>
    select(disability_pain_cal, 
           #chronic_pain_ever_cal,
           bipolar_washout_cal,
           anxiety_washout_cal,
           adhd_washout_cal,
           depression_washout_cal,
           mental_ill_washout_cal,
           antidepressant_washout_cal,
           benzodiazepine_washout_cal,
           antipsychotic_washout_cal, 
           stimulant_washout_cal,
           moodstabilizer_washout_cal,
           opioid_pain_washout_cal# ,
           # bipolar_study_cal,
           # anxiety_study_cal,
           # adhd_study_cal,
           # depression_study_cal,
           # mental_ill_study_cal,
           # antidepressant_study_cal,
           # benzodiazepine_study_cal,
           # antipsychotic_study_cal, 
           # stimulant_study_cal,
           # moodstabilizer_study_cal,
           # opioid_pain_study_cal
          
    ) |>
    labelled::set_variable_labels(
      chronic_pain_ever_cal  = "Chronic Pain Ever",
        bipolar_study_cal = "Bipolar",
        anxiety_study_cal = "Anxiety",
        adhd_study_cal = "ADD/ADHD",
        depression_study_cal = "Depression",
        mental_ill_study_cal = "Other Mental Illness",
        antidepressant_study_cal = "Antidepressant Prescription",
        benzodiazepine_study_cal = "Benzodiazepine Prescription",
        antipsychotic_study_cal = "Anti-psychotic Prescription", 
        stimulant_study_cal = "Stimulant Prescription",
        moodstabilizer_study_cal = "Mood Stabilizer Prescription",
        opioid_pain_study_cal = "Opioid Pain Prescription",
    )


oud_tbl3_data <- dat_subset  |>
    select(disability_pain_cal, 
           oud_abuse_study_cal,
           oud_hillary_study_cal,
           oud_poison_study_cal,
           oud_moud_nal_study_cal,
           oud_moud_met_study_cal,
           oud_moud_bup_study_cal,
           oud_misuse_cal_any,                     
           oud_misuse_cal_avg_distinct_providers,
           oud_misuse_cal_avg_distinct_dispensers,
           oud_misuse_cal_avg_total_days_supply,
           oud_misuse_abuse_poison_cal,
           oud_cal,
           any_moud_cal
    ) |>
    labelled::set_variable_labels(
        oud_abuse_study_cal = "OUD Diagnosis Abuse", 
        oud_hillary_study_cal = "OUD Hillary Abuse",
        oud_poison_study_cal = "OUD Diagnosis Poison",
        oud_moud_nal_study_cal  = "MOUD: Nal",
        oud_moud_met_study_cal  = "MOUD: Met",
        oud_moud_bup_study_cal   = "MOUD: Bup",
        oud_misuse_cal_any = "OUD: Probable Misuse",                     
        oud_misuse_cal_avg_distinct_providers = "OUD: Avg. Disinct Providers",
        oud_misuse_cal_avg_distinct_dispensers  = "OUD: Avg. Disinct Dispensers",
        oud_misuse_cal_avg_total_days_supply = "OUD: Avg. Days Supply",
        oud_misuse_abuse_poison_cal = "OUD: Any Abuse, Poison, or Probable Misabuse",
        oud_cal = "OUD: Any"
    )




##########  DEMOGRPAHICS ################################################## 

tic()
dem_tbl3 <-
    dem_tbl3_data |>
    tbl_summary(by = disability_pain_cal)
toc()

latex_tbl <- dem_tbl3 |>
    as_gt() |>
    as_latex()

write_rds(latex_tbl, "output/dem_tbl3_gt.rds")



dem_tbl3_df <- dem_tbl3 |>
    as_gt() |>
    as.data.frame()

dem_tbl3_df_select <-
    dem_tbl3_df |>
    select(label:stat_4)

write_csv(dem_tbl3_df, "tbls/dem_tbl3.csv")


##########  COMORBIDITIES ################################################## 

tic()
comorb_tbl3 <-
    comorb_tbl3_data |>
    tbl_summary(by = disability_pain_cal) 
toc()


latex_tbl <- comorb_tbl3 |>
    as_gt() |>
    as_latex()

write_rds(latex_tbl, "output/comorb_tbl3_gt.rds")



comorb_tbl3_df <- comorb_tbl3 |>
    as_gt() |>
    as.data.frame()

comorb_tbl3_df_select <-
    comorb_tbl3_df |>
    select(label:stat_4)

write_csv(comorb_tbl3_df, "tbls/comorb_tbl3.csv")


##########  OUD  ##########################################################  

# make table
tic()
oud_tbl3 <-
    oud_tbl3_data |>
    tbl_summary(by = disability_pain_cal) 
toc()

# convert to latex
latex_tbl <- oud_tbl3 |>
    as_gt() |>
    as_latex()

# export to latex code
write_rds(latex_tbl, "output/oud_tbl3_gt.rds")



oud_tbl3_df <- 
    oud_tbl3 |>
    as_gt() |>
    as.data.frame()

oud_tbl3_df_select <-
    oud_tbl3_df |>
    select(label:stat_4)

write_csv(oud_tbl3_df, "tbls/oud_tbl3.csv")


### extra numbers for paper

dat_subset |> janitor::tabyl(dem_sex)
# dem_sex       n   percent
# F 1200441 0.4917962
# M 1240491 0.5082038

analysis_cohort |>
    select(dem_race)|>
    tbl_summary() |>
    as_gt() |>
    as.data.frame() 

dat_subset |>
    select(dem_race, disability_pain_cal)|>
    tbl_summary(by=disability_pain_cal) |>
    as_gt() |>
    as.data.frame() 



# Create data for table 1 (SUBSET ANALYSIS COHORT, age restiriction) pieces -----------------------------------------------
