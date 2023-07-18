# clean NDC codes
library(tidyverse)
library(readxl)
library(janitor)

# function to take in a data frame containing column with 10 digit ndc codes separated by "-"
# output: an 11 digit ndc code without hyphens
ndc_conversion <- function(df, ndc_raw_name){
    clean_code <- df |>
        separate(ndc_raw_name, into=c("c1","c2","c3"), "-") |>
        mutate(c1 = case_when(str_length(c1) < 5 ~ str_pad(c1, width=5, side="left", pad="0"),
                              TRUE ~ c1),
               c2 = case_when(str_length(c2) < 4 ~ str_pad(c2, width=4, side="left", pad="0"),
                              TRUE ~ c2),
               c3 = case_when(str_length(c3) < 2 ~ str_pad(c3, width=2, side="left", pad="0"),
                              TRUE ~ c3),
               ndc_code = paste0(c1,c2,c3)) |>
        pull(ndc_code)
    return(clean_code) # return the single vector of ndc codes
}


# Antidepressants ---------------------------------------------------------

# antidepressant https://docs.google.com/spreadsheets/d/1YRmNrc1e0SpJfgtWBMm1aFusW8dIPVN5vu2fyl00CqI/edit#gid=876641283

antidepressant_raw1 <- readxl::read_xlsx("projects/create_cohort/input/NDC_codes/Antidepressant NDC Codes.xlsx") |>
    clean_names()

antidepressant_raw2 <- readxl::read_xlsx("projects/create_cohort/input/NDC_codes/Antidepressant NDC Codes.xlsx",
                                        sheet = 2) |>
    clean_names()

antidepressant_raw3 <- readxl::read_xlsx("projects/create_cohort/input/NDC_codes/Antidepressant NDC Codes.xlsx",
                                         sheet = 3) |>
    clean_names()


antidepressant_raw1 |>
    select(starts_with("x")) |>
    names()

antidepressant_raw2 |>
    select(starts_with("x")) |>
    names()

antidepressant_raw3 |>
    select(starts_with("x")) |>
    names()

r1 <- antidepressant_raw1 |>
    select(-starts_with("x")) |>
    mutate(row = 1:n())
r2 <- antidepressant_raw2 |>
    select(-starts_with("x"))|>
    mutate(row = 1:n())
r3 <- antidepressant_raw3 |>
    select(-starts_with("x"))|>
    mutate(row = 1:n())

antidepressant_codes <- 
    full_join(r1, r2) |>
    full_join(r3) |> 
    select(-row) |>
    pivot_longer(everything(),
                 names_to = "drug",
                 values_to = "ndc_raw") |>
    arrange(drug) |>
    drop_na()

antidepressant_codes$ndc <- ndc_conversion(antidepressant_codes, "ndc_raw")
antidepressant_codes

write_csv(antidepressant_codes, "projects/create_cohort/input/NDC_codes/antidepressants_clean.csv")

# Benzodiazepene ---------------------------------------------------------

# benzodiazepene https://docs.google.com/spreadsheets/d/1YRmNrc1e0SpJfgtWBMm1aFusW8dIPVN5vu2fyl00CqI/edit#gid=876641283

    benzodiazepene_raw1 <- readxl::read_xlsx("projects/create_cohort/input/NDC_codes/Benzodiazepines NDC Codes.xlsx") |>
    clean_names()

benzodiazepene_raw2 <- readxl::read_xlsx("projects/create_cohort/input/NDC_codes/Benzodiazepines NDC Codes.xlsxx",
                                         sheet = 2) |>
    clean_names()

benzodiazepene_raw3 <- readxl::read_xlsx("projects/create_cohort/input/NDC_codes/Benzodiazepines NDC Codes.xlsx",
                                         sheet = 3) |>
    clean_names()


benzodiazepene_raw4 <- readxl::read_xlsx("projects/create_cohort/input/NDC_codes/Benzodiazepines NDC Codes.xlsx",
                                         sheet = 4) |>
    clean_names()


benzodiazepene_raw5 <- readxl::read_xlsx("projects/create_cohort/input/NDC_codes/Benzodiazepines NDC Codes.xlsx",
                                         sheet = 5) |>
    clean_names()

benzodiazepene_raw1 |>
    select(starts_with("x")) |>
    names()

benzodiazepene_raw2 |>
    select(starts_with("x")) |>
    names()

benzodiazepene_raw3 |>
    select(starts_with("x")) |>
    names()

benzodiazepene_raw4 |>
    select(starts_with("x")) |>
    names()

benzodiazepene_raw5 |>
    select(starts_with("x")) |>
    names()

r1 <- benzodiazepene_raw1 |>
    select(-starts_with("x")) |>
    mutate(row = 1:n())
r2 <- benzodiazepene_raw2 |>
    select(-starts_with("x"))|>
    mutate(row = 1:n())
r3 <- benzodiazepene_raw3 |>
    select(-starts_with("x"))|>
    mutate(row = 1:n())
r4 <- benzodiazepene_raw4 |>
    select(-starts_with("x"))|>
    mutate(row = 1:n())
r5 <- benzodiazepene_raw5 |>
    select(-starts_with("x"))|>
    mutate(row = 1:n())

benzodiazepene_codes <- 
    full_join(r1, r2) |>
    full_join(r3) |> 
    full_join(r4) |> 
    full_join(r5) |> 
    select(-row) |>
    pivot_longer(everything(),
                 names_to = "drug",
                 values_to = "ndc_raw") |>
    arrange(drug) |>
    drop_na()

benzodiazepene_codes$ndc <- ndc_conversion(benzodiazepene_codes, "ndc_raw")
benzodiazepene_codes

write_csv(benzodiazepene_codes, "projects/create_cohort/input/NDC_codes/benzodiazepines_clean.csv")



# Anti-phsychotics ---------------------------------------------------------

# https://docs.google.com/spreadsheets/d/1YRmNrc1e0SpJfgtWBMm1aFusW8dIPVN5vu2fyl00CqI/edit#gid=876641283

antipsychotic_raw1 <- readxl::read_xlsx("projects/create_cohort/input/NDC_codes/Anti-Psychotic NDC Codes .xlsx") |>
    clean_names()

antipsychotic_raw2 <- readxl::read_xlsx("projects/create_cohort/input/NDC_codes/Anti-Psychotic NDC Codes .xlsx",
                                         sheet = 2) |>
    clean_names()


antipsychotic_raw1 |>
    select(starts_with("x")) |>
    names()

antipsychotic_raw2 |>
    select(starts_with("x")) |>
    names()


r1 <- antipsychotic_raw1 |>
    select(-starts_with("x")) |>
    mutate(row = 1:n())
r2 <- antipsychotic_raw2 |>
    select(-starts_with("x"))|>
    mutate(row = 1:n())


antipsychotic_codes <- 
    full_join(r1, r2) |>
    select(-row) |>
    pivot_longer(everything(),
                 names_to = "drug",
                 values_to = "ndc_raw") |>
    arrange(drug) |>
    drop_na()

antipsychotic_codes$ndc <- ndc_conversion(antipsychotic_codes, "ndc_raw")
antipsychotic_codes

write_csv(antipsychotic_codes, "projects/create_cohort/input/NDC_codes/antipsychotics_clean.csv")



# Mood Stabilizers ---------------------------------------------------------

# https://docs.google.com/spreadsheets/d/1YRmNrc1e0SpJfgtWBMm1aFusW8dIPVN5vu2fyl00CqI/edit#gid=876641283

moodstabilizer_raw1 <- readxl::read_xlsx("projects/create_cohort/input/NDC_codes/Mood Stabilizer NDC Codes .xlsx") |>
    clean_names()

moodstabilizer_raw1 |>
    select(starts_with("x")) |>
    names()

r1 <- moodstabilizer_raw1 |>
    select(-starts_with("x"))

moodstabilizer_codes <- 
    r1 |>
    pivot_longer(everything(),
                 names_to = "drug",
                 values_to = "ndc_raw") |>
    arrange(drug) |>
    drop_na()

moodstabilizer_codes$ndc <- ndc_conversion(moodstabilizer_codes, "ndc_raw")
moodstabilizer_codes

write_csv(moodstabilizer_codes, "projects/create_cohort/input/NDC_codes/moodstabilizers_clean.csv")



# Stimulants ---------------------------------------------------------

# https://docs.google.com/spreadsheets/d/1YRmNrc1e0SpJfgtWBMm1aFusW8dIPVN5vu2fyl00CqI/edit#gid=876641283

stimulants_raw1 <- readxl::read_xlsx("projects/create_cohort/input/NDC_codes/Stimulant NDC Codes .xlsx") |>
    clean_names()

stimulants_raw1 |>
    select(starts_with("x")) |>
    names()

r1 <- stimulants_raw1 |>
    select(-starts_with("x")) 

stimulant_codes <- 
    r1 |>
    pivot_longer(everything(),
                 names_to = "drug",
                 values_to = "ndc_raw") |>
    arrange(drug) |>
    drop_na()

stimulant_codes$ndc <- ndc_conversion(stimulant_codes, "ndc_raw")
stimulant_codes

write_csv(stimulant_codes, "projects/create_cohort/input/NDC_codes/stimulants_clean.csv")


# Opioid Pain Prescription ---------------------------------------------------------

# opioid_pain_rx are all in raw NDC csvs (changed for an upcoming project using unsafe pain mngmt practices)
# all contained in the folder input/NDC_codes/NDC Opioid CSVs

filepath <- "projects/create_cohort/input/NDC_codes/NDC Opioid CSVs/"

all_opioid_files <- list.files(filepath)

clean_opioid_ndcs <- function(file, direct = filepath){
    drug_name <- str_remove(word(file), ".csv")
    tmp_csv <- janitor::clean_names(read_csv(paste0(filepath, file)))
    tmp_csv$ndc <- ndc_conversion(tmp_csv, "ndc_package_code")
    tmp_csv$drug_name <- drug_name
    return(tmp_csv)
}

as.list(all_opioid_files)

opioid_pain_rx_codes_raw <- map_dfr(all_opioid_files, ~clean_opioid_ndcs(.x)) |>
    distinct() |>
    filter(!str_detect(strength, "_")) 

opioid_pain_rx_codes <-
    opioid_pain_rx_codes_raw |> # filter out cough medicines
    filter(!(drug_name == "Buprenorphine" & str_detect(dosage_form, "TABLET") & str_detect(strength, ","))) |> # these are bup + naloxone (MOUD) 
    mutate(bup_strength_clean = parse_number(strength))  # only fix bup strength for now (all we need -- for mme conversions in later projects we'll need more)

write_csv(opioid_pain_rx_codes, "projects/create_cohort/input/NDC_codes/opioid_pain_rxs_clean.csv")




