################################################################################
################################################################################
###  CLEAN DEMOOGRAPHICS BASE FILES
###  Kat Hoffman, Dec 2022
###  Purpose: clean TAFDEBSE files for all years
###  Output: new cohort data frame after removing non-eligible age, pregnancy
###         income, dual eligibility, and deaf/blind beneficiaries from relevant
###         period windows/years
################################################################################
################################################################################

# Set up -----------------------------------------------------------------------

# load libraries
library(arrow)
library(tidyverse)
library(lubridate)
library(tictoc)
library(future)
library(furrr)
library(doParallel)
# options(cores=50)
registerDoParallel()
plan(multicore)
getDoParWorkers()

# read in tafdebse data base (all years)
td <- "/home/data/12201/" # directory of interest
dbs_files <- paste0(list.files(td, pattern = "*TAFDEBSE*", recursive = TRUE)) # files of interest
dbs <- open_dataset(paste0(td, dbs_files), format="parquet", partition = "year") # arrow dataset

drv <- "../data/clean/create_cohort/"

dts_cohorts <- open_dataset(paste0(drv, "tafdedts/dts_cohorts.parquet")) |> collect()

# FILTER OUT MARYLAND --------------------------------------------------------

states <-
    dbs |> 
    select(BENE_ID, STATE_CD)  |>
    collect()

# to add in first step of creating cohort indicators
cohort_exclusion_maryland <-
    states |> 
    filter(STATE_CD == "MD") |> 
    distinct(BENE_ID) |>
    mutate(cohort_exclusion_state = 1) # fill rest with NAs

# Age indicators via birthday ----------------------------------------------------------------

# keep age and birth date columns for the first year each beneficiary appears in demographics
birth_dates_cols <-
    dbs |>
    arrange(RFRNC_YR) |>
    select(BENE_ID, BIRTH_DT, AGE) |>
    collect()

# keep only the first non NA birth dates
birth_dates <-
    birth_dates_cols |>
    drop_na(BIRTH_DT) |> # 46,894 missing a birth date before doing this
    distinct(BENE_ID, .keep_all = T) 

cohort_exclusion_age <-
    dts_cohorts |> 
    left_join(birth_dates) |>
    # note: this is slightly different from the "AGE" variable calculated by Medicaid (end of year age)
    mutate(age_enrollment = floor(as.numeric(difftime(washout_start_dt, BIRTH_DT, units = "days") / 365.25))) |>
    mutate(cohort_exclusion_age = case_when(age_enrollment < 19 ~ 1,
                                            age_enrollment >= 65 ~ 1, 
                                            TRUE ~ 0)) |>
    select(BENE_ID, cohort_exclusion_age) # remove Medicaid's age variable so as not to confuse

# exclude if missing sex (indicative that not good data on these individuals)
cohort_exclusion_sex <-
    dbs |>
    arrange(RFRNC_YR) |>
    select(BENE_ID, SEX_CD) |>
    collect() |>
    drop_na(SEX_CD) |>
    distinct(BENE_ID, .keep_all = T) |>
    right_join(dts_cohorts |> select(BENE_ID)) |>
    mutate(cohort_exclusion_sex = case_when(is.na(SEX_CD) ~ 1, TRUE ~ 0)) |> 
    select(BENE_ID, cohort_exclusion_sex)


saveRDS(cohort_exclusion_age, paste0(drv, "tafdebse/cohort_exclusion_age.rds"))
saveRDS(cohort_exclusion_maryland, paste0(drv, "tafdebse/cohort_exclusion_maryland.rds"))
saveRDS(cohort_exclusion_sex, paste0(drv,"tafdebse/cohort_exclusion_sex.rds"))


# CREATE LONG FORMAT WASHOUT PERIOD KEY ----------------------------------------

elig_codes_to_check <-
    dbs |>
    select(BENE_ID, RFRNC_YR, starts_with("ELGBLTY_GRP_CD")) |>
    arrange(BENE_ID, RFRNC_YR) |>
    collect()

# write_parquet(elig_codes_to_check, "data/tafdebse/elig_codes_to_check.parquet")

# 999.778 sec elapsed
tic()
elig_nested <-
    elig_codes_to_check |>
    group_by(BENE_ID) |>
    nest()
toc()

saveRDS(elig_nested, paste0(drv, "tafdebse/elig_nested.rds"))

elig_nested_index <-
    elig_nested |>
    ungroup() |>
    mutate(index = rep(1:100, length.out=n()))

# Save all the latest eligibility codes from the washout period
td <- paste0(drv, "tafdebse/tmp_splits_elig/" )
files <- parse_number(paste0(list.files(td, pattern = "*parquet", recursive = TRUE)))
seq <- 1:100
# still <- seq  
still <- seq[-which(seq %in% files)]
 
foreach(i = still) %dopar%
    {
        idx <- i
        
        print(paste(idx,Sys.time()))
        
elig_long <-
    elig_nested_index |>
    filter(index == i) |>
    left_join(dts_cohorts) |>
    unnest(data) |>
    rename(year = RFRNC_YR) |>
    select(-ELGBLTY_GRP_CD_LTST) |>
    pivot_longer(cols = starts_with("ELGBLTY_GRP_CD"),
                 names_to = "month",
                 values_to = "elig_code",
                 values_drop_na = T) |>
    mutate(month = parse_number(month),
           year = as.numeric(year),
           elig_dt = as.Date(paste0(year, "-", month, "-01")))

print(paste(idx,Sys.time()))

# 6 month calendar interval
last_elig_cal <-
    elig_long |>
    filter(elig_dt %within% interval(washout_start_dt, washout_cal_end_dt)) |>
    arrange(elig_dt) |>
    group_by(BENE_ID) |>
    filter(row_number() == n()) |>
    select(BENE_ID, washout_cal_elig_dt = elig_dt, washout_cal_elig_code = elig_code)

# 12 month calendar interval
last_elig_12mos_cal <-
    elig_long |>
    filter(elig_dt %within% interval(washout_start_dt, washout_12mos_end_dt)) |>
    arrange(elig_dt) |>
    group_by(BENE_ID) |>
    filter(row_number() == n()) |>
    select(BENE_ID, washout_12mos_cal_elig_dt = elig_dt, washout_12mos_cal_elig_code = elig_code)

last_elig_cont <-
    elig_long |>
    filter(elig_dt %within% interval(washout_start_dt, washout_cont_end_dt)) |>
    arrange(elig_dt) |>
    group_by(BENE_ID) |>
    filter(row_number() == n()) |>
    select(BENE_ID, washout_cont_elig_dt = elig_dt, washout_cont_elig_code = elig_code)

last_elig_washout <- full_join(last_elig_cal, last_elig_12mos_cal) |>
    full_join(last_elig_cont)

print(paste(idx,Sys.time()))

write_parquet(last_elig_washout, paste0(drv, "tafdebse/tmp_splits_elig/", idx, ".parquet"))

    }

# combine
td <- paste0(drv, "tafdebse/tmp_splits_elig/" )
files <- paste0(list.files(td, pattern = "*parquet", recursive = TRUE))
last_elig_washout <- open_dataset(paste0(td, files), format="parquet") |> collect() # arrow dataset
saveRDS(last_elig_washout, paste0(drv, "tafdebse/last_elig_washout.rds"))

# Add cohort exclusions based on eligibility
### DUAL ELIGIBLE NEEDS TO BE ALL 2 years

preg_elig_cds <- c("05", "53", "64", "68")
institution_elig_cds <- c("15", "42", "43", "44", "45")
unknown_dis_elig_cds <- c("54","55","69")
not_eligible_cds <- c("66","70") # not eligible for medical coverage
deaf_blind_elig_cds <- c("16") 
cancer_elig_cds <- c("34") # breast cancer treatment
tb_elig_cds <- c("36") # tuberculosis
dual_elig_cds <- c("23","24","25","26") 

income_elig_cds <- c(
    "08", # = Children with Title IV-E Adoption Assistance, Foster Care or Guardianship Care,
    "28", # = Reasonable Classifications of Individuals under Age 21
    "30", # = Independent Foster Care Adolescents
    "31", # = Optional Targeted Low Income Children
    "33", # = Individuals above 133% FPL under Age 65
    "50", # = Family Opportunity Act Children with Disabilities
    "56", # =  = Medically Needy Parents and Other Caretakers
    "61", # = Targeted Low-Income Children
    "62", # = Deemed Newborn
    "63", # = Children Ineligible for Medicaid Due to Loss of Income Disregards
    "65" # = Children with Access to Public Employee Coverage
)

dis_elig_cds <- c(
    "11", # = Individuals Receiving SSI
    "12", # = Aged, Blind and Disabled Individuals in 209(b) States
    "13", # = Individuals Receiving Mandatory State Supplements
    "17", # = Individuals Who Lost Eligibility for SSI/SSP Due to an Increase in OASDI Benefits in 1972
    "18", # = Individuals Who Would be Eligible for SSI/SSP but for OASDI COLA increases since April 1977
    "19", # = Disabled Widows and Widowers Ineligible for SSI due to Increase in OASDI
    "20", # = Disabled Widows and Widowers Ineligible for SSI due to Early Receipt of Social Security
    "21", # = Working Disabled under 1619(b)
    "22", # = Disabled Adult Children
    "37", # = Aged, Blind or Disabled Individuals Eligible for but Not Receiving Cash Assistance
    "38", #  = Individuals Eligible for Cash Assistance except for Institutionalization
    "39", # = Individuals Receiving Home and Community Based Services under Institutional Rules
    "40", # = Optional State Supplement Recipients — 1634 States, and SSI Criteria States with 1616 Agreements
    "41", # = Optional State Supplement Recipients — 209(b) States, and SSI Criteria States without 1616 Agreements
    "46", # = Poverty Level Aged or Disabled
    "47", # = Work Incentives Eligibility Group
    "48", # = Ticket to Work Basic Group
    "50", # = Family Opportunity Act Children with Disabilities,
    "51", # = Individuals Eligible for Home and Community-Based Services
    "52", # = Individuals Eligible for Home and Community-Based Services — Special Income Level
    "59", # = Medically Needy Aged, Blind or Disabled
    "60"  # = Medically Needy Blind or Disabled Individuals Eligible in 1973
)

non_dis_elig_cds <- c(
    "01", # = Parents and Other Caretaker Relatives
    "02", # = Transitional Medical Assistance
    "03", # = Extended Medicaid due to Earnings
    "04", # = Extended Medicaid due to Spousal Support Collections
    "06", # = Deemed Newborns
    "07", # = Infants and Children under Age 19
    "08", # = Children with Title IV-E Adoption Assistance, Foster Care or Guardianship Care
    "09", # = Former Foster Care Children
    "14", # = Individuals Who Are Essential Spouses
    "27", # = Optional Coverage of Parents and Other Caretaker Relatives
    "28", #  = Reasonable Classifications of Individuals under Age 21
    "29", #  = Children with Non-IV-E Adoption Assistance
    "30", #  = Independent Foster Care Adolescents
    "31", #  = Optional Targeted Low Income Children
    "32", # = Individuals Electing COBRA Continuation Coverage
    "33", #  = Individuals above 133% FPL under Age 65
    "35", # = Individuals Eligible for Family Planning Services
    "49", # = Ticket to Work Medical Improvements Group
    "56", #  = Medically Needy Parents and Other Caretakers
    "61", #  = Targeted Low-Income Children
    "62", #  = Deemed Newborn
    "63", #  = Children Ineligible for Medicaid Due to Loss of Income Disregards
    "65", #  = Children with Access to Public Employee Coverage
    "67", #  = Targeted Low-Income
    "70", # = Family Planning Participants (expansion group)
    "71", # = Other expansion group
    "72", # = Adult Group - Individuals at or below 133% FPL,19-64, newly eligible for all states
    "73", # = Adult Group - Individuals at or below 133% FPL,19-64, not newly eligible for non 1905z(3) states
    "74", # = Adult Group - Individuals at or below 133% FPL,19-64, not newly eligible parent/ caretaker-relative(s) in 1905z(3) states
    "75"  # = Adult Group - Individuals at or below 133% FPL,19-64, not newly eligible nonparent/ caretaker-relative(s) in 1905z(3) states
)


elig_clean_tmp <- 
    last_elig_washout |>
    ungroup() |>
    mutate(cohort_exclusion_cal_pregnancy =  case_when(washout_cal_elig_code %in% preg_elig_cds ~ 1,
                                                       TRUE ~ 0),
           cohort_exclusion_cal_pregnancy =  case_when(washout_12mos_cal_elig_code %in% preg_elig_cds ~ 1,
                                                       TRUE ~ 0),
           cohort_exclusion_cont_pregnancy =  case_when(washout_cont_elig_code %in% preg_elig_cds ~ 1,
                                                       TRUE ~ 0),
           cohort_exclusion_cal_institution = case_when(washout_cal_elig_code %in% institution_elig_cds ~ 1,
                                                        TRUE ~ 0),
           cohort_exclusion_12mos_cal_institution = case_when(washout_12mos_cal_elig_code %in% institution_elig_cds ~ 1,
                                                        TRUE ~ 0),
           cohort_exclusion_cont_institution = case_when(washout_cont_elig_code %in% institution_elig_cds ~ 1,
                                                        TRUE ~ 0),
           cohort_exclusion_cal_unknown_disability = case_when(washout_cal_elig_code %in% unknown_dis_elig_cds ~ 1,
                                                        TRUE ~ 0),
           cohort_exclusion_12mos_cal_unknown_disability = case_when(washout_12mos_cal_elig_code %in% unknown_dis_elig_cds ~ 1,
                                                               TRUE ~ 0),
           cohort_exclusion_cont_unknown_disability = case_when(washout_cont_elig_code %in% unknown_dis_elig_cds ~ 1,
                                                        TRUE ~ 0),
           cohort_exclusion_cal_not_elig = case_when(washout_cal_elig_code %in% not_eligible_cds ~ 1,
                                                     TRUE ~ 0),
           cohort_exclusion_12mos_cal_not_elig = case_when(washout_12mos_cal_elig_code %in% not_eligible_cds ~ 1,
                                                     TRUE ~ 0),
           cohort_exclusion_cont_not_elig = case_when(washout_cont_elig_code %in% not_eligible_cds ~ 1,
                                                      TRUE ~ 0),
           cohort_exclusion_cal_deafblind = case_when(washout_cal_elig_code %in% deaf_blind_elig_cds ~ 1,
                                                        TRUE ~ 0),
           cohort_exclusion_12mos_cal_deafblind = case_when(washout_12mos_cal_elig_code %in% deaf_blind_elig_cds ~ 1,
                                                      TRUE ~ 0),
           cohort_exclusion_cont_deafblind = case_when(washout_cont_elig_code %in% deaf_blind_elig_cds ~ 1,
                                                        TRUE ~ 0))

elig_clean_tmp1 <- 
    elig_clean_tmp |>
    mutate(
           cohort_exclusion_cal_cancer_elig = case_when(washout_cal_elig_code %in% cancer_elig_cds ~ 1,
                                                      TRUE ~ 0),
           cohort_exclusion_12mos_cal_cancer_elig = case_when(washout_12mos_cal_elig_code %in% cancer_elig_cds ~ 1,
                                                        TRUE ~ 0),
           cohort_exclusion_cont_cancer_elig = case_when(washout_cont_elig_code %in% cancer_elig_cds ~ 1,
                                                       TRUE ~ 0),
           cohort_exclusion_cal_tb = case_when(washout_cal_elig_code %in% tb_elig_cds ~ 1,
                                                        TRUE ~ 0),
           cohort_exclusion_12mos_cal_tb = case_when(washout_12mos_cal_elig_code %in% tb_elig_cds ~ 1,
                                               TRUE ~ 0),
           cohort_exclusion_cont_tb = case_when(washout_cont_elig_code %in% tb_elig_cds ~ 1,
                                                         TRUE ~ 0),
           cohort_exclusion_cal_no_elig = case_when(is.na(washout_cal_elig_code) ~ 1,
                                                     TRUE ~ 0),
           cohort_exclusion_12mos_cal_no_elig = case_when(is.na(washout_12mos_cal_elig_code) ~ 1,
                                                    TRUE ~ 0),
           cohort_exclusion_cont_no_elig = case_when(is.na(washout_cont_elig_code) ~ 1,
                                                TRUE ~ 0))

elig_clean <-
    elig_clean_tmp1 |>
    mutate(
           # DUAL ELIG STATUS BASED ON LATEST WASHOUT PERIOD CODE  (this may need to be ever in washout)
           cohort_exclusion_cal_dual = case_when(washout_cal_elig_code %in% dual_elig_cds ~ 1,
                                                    TRUE ~ 0),
           cohort_exclusion_12mos_cal_dual = case_when(washout_12mos_cal_elig_code %in% dual_elig_cds ~ 1,
                                                 TRUE ~ 0),
           cohort_exclusion_cont_dual = case_when(washout_cont_elig_code %in% dual_elig_cds ~ 1,
                                                     TRUE ~ 0),
           
           # DETERMINE DISABILITY STATUS
           disability_washout_cal = case_when(washout_cal_elig_code %in% dis_elig_cds ~ 1,
                                              washout_cal_elig_code %in% non_dis_elig_cds ~ 0),
           disability_washout_12mos_cal = case_when(washout_12mos_cal_elig_code %in% dis_elig_cds ~ 1,
                                              washout_12mos_cal_elig_code %in% non_dis_elig_cds ~ 0),
           disability_washout_cont = case_when(washout_cont_elig_code %in% dis_elig_cds ~ 1,
                                               washout_cont_elig_code %in% non_dis_elig_cds ~ 0),
           
           # INCOME BASED ON LATEST WASHOUT PERIOD CODE 
           probable_high_income_cal =case_when(washout_cal_elig_code %in% income_elig_cds ~ 1,
                                            TRUE ~ 0),
           probable_high_income_12mos_cal =case_when(washout_12mos_cal_elig_code %in% income_elig_cds ~ 1,
                                               TRUE ~ 0),
           probable_high_income_cont =case_when(washout_cont_elig_code %in% income_elig_cds ~ 1,
                                               TRUE ~ 0)
           )

# check that all eligibility codes are accounted for
elig_clean |>
    select(BENE_ID, contains("cal")) |> 
    filter_at(vars(contains("cohort_excl")), all_vars(. == 0)) |>
    filter(is.na(disability_washout_cal)) |>
    count(washout_cal_elig_code)

elig_clean |>
    select(BENE_ID, contains("cont")) |> 
    filter_at(vars(contains("cohort_excl")), all_vars(. == 0)) |>
    filter(is.na(disability_washout_cont)) |>
    count(washout_cont_elig_code)

write_rds(elig_clean, paste0(drv, "tafdebse/cohort_exclusion_elig_cds.rds"))

# CREATE LONG FORMAT RELEVANT ELIGIBILITY CODES --------------------------------

# FILTER OUT DUAL ELIGIBLES USING DUAL_ELGBL_CD ----------------------------------------------------

tic()
dual_codes_to_check <-
  dbs |>
  select(BENE_ID, RFRNC_YR, starts_with("DUAL_ELGBL_CD")) |>
  collect()

# obtain the date for all dual eligibility encounters for all beneficiaries
all_duals <-
  dual_codes_to_check |>
  rename(year = RFRNC_YR) |>
  select(-DUAL_ELGBL_CD_LTST) |>
  pivot_longer(cols = starts_with("DUAL_ELGBL_CD"),
               names_to = "month",
               values_to = "dual_code",
               values_drop_na = T)  |>
  filter(dual_code != "00") |>
  mutate(month = parse_number(month),
         year = as.numeric(year),
         dual_elig_date = as.Date(paste0(year, "-", month, "-01"))) |>
  select(BENE_ID,  dual_elig_date, dual_code)
toc() #599.57 sec elapsed

tic()
cohort_exclusion_cal_dual <-
    all_duals  |>
        inner_join(dts_cohorts) |>
        mutate(cohort_exclusion_cal_dual = case_when(dual_elig_date %within% interval(washout_start_dt, washout_cal_end_dt) ~ 1)) |>
        filter(cohort_exclusion_cal_dual == 1) |>
        select(BENE_ID,  cohort_exclusion_cal_dual_elig = cohort_exclusion_cal_dual) |>
        distinct()

cohort_exclusion_12mos_cal_dual <-
    all_duals  |>
    inner_join(dts_cohorts) |>
    mutate(cohort_exclusion_12mos_cal_dual = case_when(dual_elig_date %within% interval(washout_start_dt, washout_12mos_end_dt) ~ 1)) |>
    filter(cohort_exclusion_12mos_cal_dual == 1) |>
    select(BENE_ID,  cohort_exclusion_12mos_cal_dual_elig = cohort_exclusion_12mos_cal_dual) |>
    distinct()

cohort_exclusion_cont_dual <-   
    all_duals  |>
    inner_join(dts_cohorts) |>
    mutate(cohort_exclusion_cont_dual = case_when(dual_elig_date %within% interval(washout_start_dt, washout_cont_end_dt) ~ 1)) |>
    filter(cohort_exclusion_cont_dual == 1) |>
    select(BENE_ID, cohort_exclusion_cont_dual_elig = cohort_exclusion_cont_dual) |>
    distinct()
toc()

saveRDS(cohort_exclusion_cal_dual, paste0(drv, "tafdebse/cohort_exclusion_cal_dual.rds"))
saveRDS(cohort_exclusion_12mos_cal_dual, paste0(drv, "tafdebse/cohort_exclusion_12mos_cal_dual.rds"))
saveRDS(cohort_exclusion_cont_dual, paste0(drv, "tafdebse/cohort_exclusion_cont_dual.rds"))

# FILTER OUT SEVERE DEAF/BLIND --------------------------------------------------------

# get relevant columns to check (deaf and blind columns binned yearly)
deafblind_dat <-
  dbs |>
  select(BENE_ID, RFRNC_YR, DSBLTY_BLND_IND, DSBLTY_DEAF_IND) |>
  collect() 

# create a data frame that contains deaf and blind patients and year
deafblind_washout_yr <- 
  deafblind_dat |>
  filter(DSBLTY_BLND_IND == 1 | DSBLTY_DEAF_IND == 1) |> # lots of NAs, not useful data for this temp data frame
  mutate(db_year = as.numeric(RFRNC_YR)) |> # rename as the income year for this temp data set
  arrange(db_year) |>
  distinct(BENE_ID, .keep_all = T) |> # keep only the first year that appears in the data set
  inner_join(dts_cohorts) |> # merge previous cohort
  mutate(washout_year = year(washout_start_dt)) |>
  filter(db_year == washout_year) |> # only keep deaf/blind data relevant to the washout *year* (limitation, but best we can do i think)
  select(BENE_ID, cohort_exclusion_blind = DSBLTY_BLND_IND, cohort_exclusion_deaf = DSBLTY_DEAF_IND)

saveRDS(deafblind_washout_yr, paste0(drv, "tafdebse/cohort_exclusion_deaf_blind.rds")) # save to merge - next step is to remove intellectual disabilities

### Extract additional variables

dts_cohorts <- open_dataset(paste0(drv, "tafdedts/dts_cohorts.parquet")) |>
    collect() 

#       - age
#       - Sex
#       - Race
#       - Race Expanded
#       - Primary language 
#       - Primary languague group
#       - Income 
#       - County Code
#       - Ethnicity
#       - Veteran
#       - Marital Status 
#       - Household size
#       - Work Status
#       - Other federal benefits
#       - Insurance Status 
#       - Physical disability
#       - No physical disability (individuals who qualify under Medicaid expansion)

work_codes <- c(
    21, # = Working Disabled under 1619(b)
    24, # = Qualified Disabled and Working Individuals
    47, # = Work Incentives Eligibility Group
    48, # = Ticket to Work Basic Group
    49  # = Ticket to Work Medical Improvements Group
)

dems_query <- 
    dbs |>
    select(BENE_ID,
           RFRNC_YR,
           BENE_STATE_CD,
           BENE_CNTY_CD,
           SEX_CD,
           ETHNCTY_CD,
           RACE_ETHNCTY_CD,
           RACE_ETHNCTY_EXP_CD,
           PRMRY_LANG_GRP_CD,
           PRMRY_LANG_CD,
           INCM_CD,
           VET_IND,
           HSEHLD_SIZE_CD,
           MRTL_STUS_CD,
           TANF_CASH_CD,
           SSI_STATE_SPLMT_CD,
           TPL_INSRNC_CVRG_IND
           )
# |>
#     arrange(BENE_ID, RFRNC_YR) |>
#     select(-ELGBLTY_GRP_CD_LTST) |>
#     collect() |>
#     group_by(BENE_ID) |>
#     filter(row_number() == 1) 


col_names <- names(dems_query)

fill_vals <- function(query_name, col_name){
    print(col_name)
    df <- 
        query_name |> # can't hold the collected version in global mem with future_map
        collect() |>
        select(BENE_ID, RFRNC_YR, one_of(col_name))  |> # pull the id, year, and col name of interest
        drop_na() |> # drop all missing values 
        group_by(BENE_ID) |>
        arrange(RFRNC_YR) |>
        filter(row_number() == 1) |> # keep only the first non-missing value for each bene_id
        select(-RFRNC_YR) |> # don't need ref year anymore
        ungroup() 
    return(df) # return the id and dem column of interest for everyone who has a value for it
}

# find available cores (64) for future
no_cores <- availableCores() - 1
plan(multicore, workers = no_cores)

# send out the mapping function to 63 cores to "fill" in values for these variables (ie only keep first non missing value, for each column)
tic()
all_dem_dfs <- future_map(col_names[3:length(col_names)], ~fill_vals(query_name = dems_query,
                                                                     col_name = .x))
toc() # 1270.785 sec elapsed

tic()
# this outputs a list of bene_id and each column
dem_df <- reduce(all_dem_dfs, ~full_join(.x, .y)) # join all the demographics we have together (by BENE_ID)
toc() # 292 sec elapsed

dem_df_clean <- 
    dem_df |>
    mutate(cohort_exclusion_no_location = case_when(is.na(BENE_STATE_CD) ~ 1,
                                                    is.na(STATE_CD) ~ 1,
                                                    TRUE ~ 0))

# note that when i read this in, will need to replace missing people with zeros
write_parquet(dem_df_clean, paste0(drv, "tafdebse/dem_df.parquet"))

# checked before this, no instances in which death_dt is NA but death_ind == 1
death <- dbs |>
    select(BENE_ID, RFRNC_YR, DEATH_DT, DEATH_IND) |>
    filter(!is.na(DEATH_DT), !is.na(BENE_ID)) |> 
    collect()

# 78 people with two diff death dates, taking the first 
death_clean <- death |>
    arrange(DEATH_DT) |>
    select(BENE_ID, death_dt = DEATH_DT) |>
    distinct(BENE_ID, .keep_all = T) |>
    right_join(dts_cohorts |> select(BENE_ID)) |>
    mutate(death_ever = case_when(is.na(death_dt) ~ 0,
                                  !is.na(death_dt) ~ 1))

saveRDS(death_clean, paste0(drv, "final/death_dts.rds"))

