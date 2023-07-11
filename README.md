# disability-chronic-pain
Analyzing associations between disability / chronic pain and incident opioid use disorder in 2016-2019 CMS data.

- `scripts/01_clean_tafdedts.R` defines dates of enrollment via the TAFDEDTS (Demographics - Dates) CMS file. For the analysis, enrollment time was counted in calendar years (relevant variables `washout_start_dt` = washout start date, `washout_cal_end_dt` = washout end date (6 months after washout start date), `study_cal_end_dt` = study end date (24 months after washout start date)). *Note:* There are other variables for study duration that have `_cont` suffixes. This is an alternate way of defining the study duration and can be ignored.

# Eligibility criteria

- `scripts/02_clean_tafdebse.R` cleans the TAFDEBSE (Demographics Base) CMS file. Creates several basic study eligibility indicators (e.g. age, state, sex) and uses eligibility codes to create more complex study eligibility indicators (e.g. dual eligible status, pregnancy).

- `scripts/03_initial_cohort_exclusions/clean_tafoth.R` and `scripts/03_initial_cohort_exclusions/clean_tafihp.R` determines whether the beneficiary had remaining exclusion criterias (e.g. severe intellectual disabilities, dementia, schizophrenia) in the washout period using the Other Services and Inpatient Hospital TAF files' ICD-10 codes.

	- The specific ICD-10 codes used for exclusion criteria can be found in `scripts/ICD_codes/disabilities.R`, with the exception of palliative care, which is directly listed in the cleaning files (ICD10 code "Z515").

- `scripts/03_initial_cohort_exclusions/add_LTC_variables.R` creates a long term care exclusion indicator by categorizing the `PTNT_DSCHRG_STUS_CD` in the Inpatient Hospital file.

# Baseline variables

# Psychiatric Comorbidity ICD-10 codes

# Concurrent Prescription NDC codes

# OUD ICD-10 diagnosis codes

# MOUD extraction

# OUD definitions