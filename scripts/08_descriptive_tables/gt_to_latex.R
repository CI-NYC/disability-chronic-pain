################################################################################
################################################################################
###  EXPORT GT RDS OBJECTS AS LATEX CODE
###  Kat Hoffman, MAY 2023
###  Output: LaTeX code pasted to console, to copy into overleaf doc for paper
################################################################################
################################################################################

library(tidyverse)

# tables to latex

fp <- "projects/create_cohort/output/tbls/gt/"

tmp <- read_rds(here::here(fp, "dem_tbl_gt.rds"))
tmp %>%
    as.character() %>%
    cat()

tmp <- read_rds(here::here(fp, "comorb_tbl_gt.rds"))
tmp %>%
    as.character() %>%
    cat()

tmp <- read_rds(here::here(fp, "oud_tbl_gt.rds"))
tmp %>%
    as.character() %>%
    cat()

tmp <- read_rds(here::here(fp, "dem_tbl2_gt.rds"))
tmp %>%
    as.character() %>%
    cat()

tmp <- read_rds(here::here(fp, "comorb_tbl2_gt.rds"))
tmp %>%
    as.character() %>%
    cat()

tmp <- read_rds(here::here(fp, "oud_tbl2_gt.rds"))
tmp %>%
    as.character() %>%
    cat()


tmp <- read_rds(here::here(fp, "missingness_tbl_gt.rds"))
tmp %>%
    as.character() %>%
    cat()



tmp <- read_rds(here::here(fp, "missingness_tbl2_gt.rds"))
tmp %>%
    as.character() %>%
    cat()



tmp <- read_rds(here::here(fp, "oud_tbl3_gt.rds"))
tmp %>%
    as.character() %>%
    cat()



tmp <- read_rds(here::here(fp, "dem_tbl3_gt.rds"))
tmp %>%
    as.character() %>%
    cat()

tmp <- read_rds(here::here(fp, "comorb_tbl3_gt.rds"))
tmp %>%
    as.character() %>%
    cat()

tmp <- read_rds(here::here(fp, "oud_tbl3_gt.rds"))
tmp %>%
    as.character() %>%
    cat()

