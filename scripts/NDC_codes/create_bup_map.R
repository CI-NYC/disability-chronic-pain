# creating a map between bup csv (lots of info) and dosing to scp into aws server
# NOT run on server

library(tidyverse)

fp <- "projects/create_cohort/input/NDC_codes/"

bup_info_raw <- read_csv(paste0(fp, "bup_ndc_raw.csv"))
bup_info <-
    bup_info_raw |>
    rename(ndc_code = `NDC Package Code`, strength = Strength, dosage_form = `Dosage Form`)

source(here::here("projects/create_cohort/scripts/NDC_codes/buprenorphine.R")) # note this sourcing is relative to my local file paths, not the server

bup_ndc_dose <-
    bup_info |>
    filter(ndc_code %in% buprenorphine) |>
    filter(str_detect(dosage_form, "TABLET")) |>
    mutate(strength_clean = parse_number(strength)) |>
    rename(ndc_code_noformat = ndc_code)

ndc_conversion <- function(code_list, code_list_name){
    code_df <- data.frame(code_list[code_list_name])
    # return a data frame of the cleaned NDC codes
    clean_code <- 
        code_df |>
        separate(code_list_name, into=c("c1","c2","c3"), "-") |>
        mutate(c1 = case_when(str_length(c1) < 5 ~ str_pad(c1, width=5, side="left", pad="0"),
                              TRUE ~ c1),
               c2 = case_when(str_length(c2) < 4 ~ str_pad(c2, width=4, side="left", pad="0"),
                              TRUE ~ c2),
               c3 = case_when(str_length(c3) < 2 ~ str_pad(c3, width=2, side="left", pad="0"),
                              TRUE ~ c3),
               newcode = paste0(c1,c2,c3))
    return(clean_code$newcode) # return the single vector of ndc codes
}


bup_ndc_dose$ndc_code <- ndc_conversion(bup_ndc_dose, "ndc_code_noformat")

write_rds(bup_ndc_dose, here::here(fp, "bup_ndc_dose_clean.rds"))

