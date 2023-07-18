################################################################################
################################################################################
### Clean NDC Prescription codes for later use
### Kat Hoffman, Jan 2023
################################################################################
################################################################################

library(tidyverse)

# source individual vectors of ndc codes 
source("scripts/NDC_codes/buprenorphine.R")
source("scripts/NDC_codes/naltrexone.R")
source("scripts/NDC_codes/hydrocodone.R")
source("scripts/NDC_codes/oxycodone.R")
source("scripts/NDC_codes/fentanyl.R")
source("scripts/NDC_codes/codeine.R")
source("scripts/NDC_codes/hydromorphone.R")
source("scripts/NDC_codes/morphine.R")
source("scripts/NDC_codes/methadone.R")
source("scripts/NDC_codes/other_opioids.R")



# create a single list for cleaning
ndc_list <- list(
        "codeine" = codeine,
        "fentanyl" = fentanyl,
        "hydrocodone" = hydrocodone,
        "hydromorphone" = hydromorphone,
        "morphine" = morphine,
        "oxycodone" = oxycodone,
        "buprenorphine" = buprenorphine,
        "naltrexone" = naltrexone,
        "methadone" = methadone,
        "alfentanilk" = alfentanilk,
        "levorphanol" = levorphanol,
        "meperidine"   = meperidine,
        "nalbuphine" = nalbuphine,
        "opium" = opium,
        "oxymorphone" = oxymorphone,
        "pentazocine"  = pentazocine,
        "remifentanil" = remifentanil,
        "sufentanil" = sufentanil,
        "tapentadol" = tapentadol,
        "tramadol"  = tramadol
        
)

# function to take in a data frame containing column with 10 digit ndc codes separated by "-"
# output: an 11 digit ndc code without hyphens
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

# run cleaning function on all the NDC code vectors
ndc_list_clean <-
    map(names(ndc_list), ~ndc_conversion(ndc_list, .x)) |>
         set_names(names(ndc_list))

# save cleaned list of NDC codes
write_rds(ndc_list_clean, "input/NDC_codes/ndc_list_clean.rds")
