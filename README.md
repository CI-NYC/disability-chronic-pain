# disability-chronic-pain

Code repository for analyzing associations between disability / chronic pain and incident opioid use disorder in 2016-2019 CMS data as described in *Hoffman, Milazzo, Williams, et al. (2023)*.

# Study duration

- `scripts/01_clean_tafdedts.R` defines dates of enrollment via the TAFDEDTS (Demographics - Dates) CMS file. For the analysis, enrollment time was counted in calendar years (relevant variables `washout_start_dt` = washout start date, `washout_cal_end_dt` = washout end date (6 months after washout start date), `study_cal_end_dt` = study end date (24 months after washout start date)). *Note:* There are other variables for study duration that have `_cont` suffixes. This is an alternate way of defining the study duration and can be ignored.

# Eligibility criteria

- `scripts/02_clean_tafdebse.R` cleans the TAFDEBSE (Demographics Base) CMS file. Creates several basic study eligibility indicators (e.g. age, state, sex) and uses eligibility codes to create more complex study eligibility indicators (e.g. dual eligible status, pregnancy).

- `scripts/03_initial_cohort_exclusions/clean_tafoth.R` and `scripts/03_initial_cohort_exclusions/clean_tafihp.R` determines whether the beneficiary had remaining exclusion criterias (e.g. severe intellectual disabilities, dementia, schizophrenia) in the washout period using the Other Services and Inpatient Hospital TAF files' ICD-10 codes.

	- The specific ICD-10 codes used for exclusion criteria can be found in `scripts/ICD_codes/disabilities.R`, with the exception of palliative care, which is directly listed in the cleaning files (ICD10 code "Z515").

- `scripts/03_initial_cohort_exclusions/add_LTC_variables.R` creates a long term care exclusion indicator by categorizing the `PTNT_DSCHRG_STUS_CD` in the Inpatient Hospital file.

# Variable definitions

## Demographics

## Psychiatric Comorbidities

The ICD-10 codes used to define ADHD, Anxiety, Bipolar, Depression, and Other Mental Illness psychiatric comorbidities can be found in `input/ICD_codes` in individual csv files. The R script to search these ICD-10 codes in Other Services and Inpatient Hospital files is in `scripts/04_define_comorbidity_vars.R`.

## Concurrent Prescriptions

The NDC codes used to concurrent prescriptions can be found in `input/NDC_codes` in individual csv files. The R script to search these NDCs in Inpatient Hospital and Pharmacy files is in `scripts/05_define_prescription_vars.R`.

## OUD

### Diagnosis codes

### MOUD

### Probable misuse

### Composite OUD outcome

# Additional notes

At times the CMS data files were too large to analyze all 17 million + beneficiaries at once. The solution to this was to create partitions, e.g. filter Other Services to only the records of 1 million beneficiaries, and then run data cleaning processes on this partition before recombining.

