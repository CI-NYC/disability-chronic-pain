# clean negative control resutls

library(tidyverse)
library(gt)
psi <- read_rds(here::here("projects/create_cohort/output/psis/psi_negcontrol_ear.rds"))


tbl_negcontrol <- tibble(est = psi$psi, se = unlist(psi$std.error), shift = names(psi$psi)) |>
    mutate(outcome = "Negative control", ic = 1)
tbl_negcontrol

for (i in 1:5){
    tbl_negcontrol$ic[[i]] <- list(psi$ic[[i]])
}

tbl_negcontrol |>
    mutate(low = est - se*1.96,
       high = est + se*1.96
) |>
    mutate(shift = case_when(shift == "A" ~ "Null",
                             shift == "1" ~ "Disability and chronic pain",
                             shift == "2" ~ "Chronic pain only",
                             shift == "3" ~ "Disability only",
                             shift == "4" ~ "Neither")) |>
    select(outcome, shift, est, low, high) |>
    filter(shift != "Null") |>
    gt() |>
    fmt_number(columns = c(est, low, high), scale_by = 100) |>
    cols_merge(
        columns = c(est, low, high),
        pattern = "{1} ({2}&mdash;{3})"
    )|>
    tab_header("Incidence estimates",
               subtitle ="Ear Infection")


# appendicitis ------------------------------------------------------------

library(tidyverse)
library(gt)
psi <- read_rds(here::here("projects/create_cohort/output/psis/psi_negcontrol_app.rds"))


tbl_negcontrol <- tibble(est = psi$psi, se = unlist(psi$std.error), shift = names(psi$psi)) |>
    mutate(outcome = "Negative control", ic = 1)
tbl_negcontrol

for (i in 1:5){
    tbl_negcontrol$ic[[i]] <- list(psi$ic[[i]])
}

tbl_negcontrol |>
    mutate(low = est - se*1.96,
           high = est + se*1.96
    ) |>
    mutate(shift = case_when(shift == "A" ~ "Null",
                             shift == "1" ~ "Disability and chronic pain",
                             shift == "2" ~ "Chronic pain only",
                             shift == "3" ~ "Disability only",
                             shift == "4" ~ "Neither")) |>
    select(outcome, shift, est, low, high) |>
    filter(shift != "Null") |>
    gt() |>
    fmt_number(columns = c(est, low, high), scale_by = 100) |>
    cols_merge(
        columns = c(est, low, high),
        pattern = "{1} ({2}&mdash;{3})"
    )|>
    tab_header("Incidence estimates",
               subtitle ="Appendicitis")


# appendicitis ------------------------------------------------------------

library(tidyverse)
library(gt)
psi <- read_rds(here::here("projects/create_cohort/output/psis/psi_negcontrol_app_exp.rds"))


tbl_negcontrol <- tibble(est = psi$psi, se = unlist(psi$std.error), shift = names(psi$psi)) |>
    mutate(outcome = "OUD Diagnosis Codes (Hillary)", ic = 1)
tbl_negcontrol

for (i in 1:3){
    tbl_negcontrol$ic[[i]] <- list(psi$ic[[i]])
}

tbl_negcontrol |>
    mutate(low = est - se*1.96,
           high = est + se*1.96
    ) |>
    # mutate(shift = case_when(shift == "A" ~ "Null",
    #                          shift == "1" ~ "Disability and chronic pain",
    #                          shift == "2" ~ "Chronic pain only",
    #                          shift == "3" ~ "Disability only",
    #                          shift == "4" ~ "Neither")) |>
    select(outcome, shift, est, low, high) |>
    filter(shift != "Null") |>
    gt() |>
    fmt_number(columns = c(est, low, high), scale_by = 100) |>
    cols_merge(
        columns = c(est, low, high),
        pattern = "{1} ({2}&mdash;{3})"
    )|>
    tab_header("Incidence estimates",
               subtitle ="Appendicitis")


# asphyxia ----------------------------------------------------------------

psi <- read_rds(here::here("projects/create_cohort/output/psis/psi_negcontrol_asph.rds"))


tbl_negcontrol <- tibble(est = psi$psi, se = unlist(psi$std.error), shift = names(psi$psi)) |>
    mutate(outcome = "Negative control", ic = 1)
tbl_negcontrol

for (i in 1:5){
    tbl_negcontrol$ic[[i]] <- list(psi$ic[[i]])
}

tbl_negcontrol |>
    mutate(low = est - se*1.96,
           high = est + se*1.96
    ) |>
    mutate(shift = case_when(shift == "A" ~ "Null",
                             shift == "1" ~ "Disability and chronic pain",
                             shift == "2" ~ "Chronic pain only",
                             shift == "3" ~ "Disability only",
                             shift == "4" ~ "Neither")) |>
    select(outcome, shift, est, low, high) |>
    filter(shift != "Null") |>
    gt() |>
    fmt_number(columns = c(est, low, high), scale_by = 100) |>
    cols_merge(
        columns = c(est, low, high),
        pattern = "{1} ({2}&mdash;{3})"
    )|>
    tab_header("Incidence estimates",
               subtitle ="Asphyxia")




