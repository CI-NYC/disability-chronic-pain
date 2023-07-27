# make forest plots of analysis on local

library(tidyverse)
library(patchwork)
library(gt)

# all_res_tbl <- read_rds(here::here("projects/create_cohort/output/all_res_tbl.rds"))

psi_days <- read_rds(here::here("projects/create_cohort/output/psis/psi_days_supereff.rds"))

summary(psi_days)
head(psi_days$psi)
summary(psi_days$g)
psi_days$std.error
summary(psi_days$ic[["2"]])

# psi_any_moud <- read_rds(here::here("projects/create_cohort/output/psis/psi_any_moud.rds"))

tbl_days <- tibble(est = psi_oud$psi, se = unlist(psi_oud$std.error), shift = names(psi_oud$psi)) |>
    mutate(outcome = "Days supply", ic = 1)


for (i in 1:5){
    tbl_days$ic[[i]] <- list(psi_days$ic[[i]])
}
tbl_days

all_res <-
    tbl_days |>
    mutate(low = est - se*1.96,
           high = est + se*1.96
    ) |>
    mutate(shift = case_when(shift == "A" ~ "Null",
                             shift == "1" ~ "Disability and chronic pain",
                             shift == "2" ~ "Chronic pain only",
                             shift == "3" ~ "Disability only",
                             shift == "4" ~ "Neither")) |>
    select(outcome, shift, est, low, high, se, ic)

contrasts <- function(outcome_name, shift_name, ref_name){
    shift_df <- all_res |>
        filter(outcome == outcome_name, shift == shift_name)
    ref_df <-  all_res |>
        filter(outcome == outcome_name & shift == ref_name)
    eif <- unlist(shift_df$ic) - unlist(ref_df$ic)
    contrast_df <- tibble(
        outcome = outcome_name,
        shift = shift_name,
        ref = ref_name,
        theta = shift_df$est - ref_df$est,
        se = sqrt(var(eif)/length(eif)),
        low = theta - se * qnorm(0.975),
        high = theta + se * qnorm(0.975),
        p.value = pnorm(abs(theta)/se, lower.tail = FALSE) * 2
        )
    return(contrast_df)
}

contrasts("OUD", "Disability and chronic pain", "Disability only")

contrast_list <- list(
    c("Disability and chronic pain", "Disability only"),
    c("Disability and chronic pain", "Chronic pain only"),
    c("Disability and chronic pain", "Neither"),
    c("Disability only", "Neither"),
    c("Chronic pain only", "Neither")
)
contrast_list


outcome_list <-
    as.list(rep(c("Days supply"), each = 5))
outcome_list


contrast_tbl <- map2_dfr(outcome_list, contrast_list, ~contrasts(.x, .y[[1]], .y[[2]]))
contrast_tbl

all_res |>
    select(shift, est, low, high) |>
    filter(shift != "Null") |>
    gt() |>
    fmt_number(columns = c(est, low, high)) |>
    cols_merge(
        columns = c(est, low, high),
        pattern = "{1} ({2}&mdash;{3})"
    ) # |>
    as_latex() |>
    as.character() |>
    cat()


contrast_tbl |>
    filter(ref == "Neither") |>
    select(outcome,shift, est = theta,low, high) |>
    gt() |>
    fmt_number(columns = c(est, low, high)) |>
    cols_merge(
        columns = c(est, low, high),
        pattern = "{1} ({2}&mdash;{3})"
    )|>
    tab_header("Disability and Chronic Pain compared to neither",
               subtitle ="Primary exposure (6 months CP)")



contrast_tbl |>
    filter(ref != "Neither") |>
    select(outcome, ref, est = theta, low, high) |>
    gt() |>
    fmt_number(columns = c(est, low, high)) |>
    cols_merge(
        columns = c(est, low, high),
        pattern = "{1} ({2}&mdash;{3})"
    ) |>
    tab_header("Disability and Chronic Pain compared to alone groups",
               subtitle ="Primary exposure (6 months CP)")




# free scales
p_est <-
    all_res |>
    filter(outcome != "Any MOUD") |>
    mutate(est = est * 100,
           low = low * 100,
           high = high * 100,
           shift = fct_rev(fct_relevel(shift, "Disability and chronic pain", "Chronic pain only")),
           outcome = fct_relevel(outcome, "OUD Hillary")) |>
    filter(shift != "Null") |>
    ggplot(aes(est, shift, xmin = low, xmax = high)) +
    geom_linerange() +
    geom_point(size=2, shape = 15) +
    # scale_x_continuous(labels = scales::percent) +
    facet_grid(.~outcome, scales = "free_x") +
    theme_bw() +
    theme(strip.background = element_blank(),
         # strip.text = element_text(face = "bold", size=12),
          axis.text.y = element_text(size = 9),
          axis.text.x = element_text(size = 8),
          axis.title.x = element_text(size= 9, margin=margin(t=5)),
          text=element_text(family="Times")
          ) +
    labs(y = "", x = "Incidence (%)") #, title= "OUD Incidence by Disability and Pain")

p_neither <- 
    contrast_tbl |>
    filter(outcome != "Any MOUD") |>
    filter(ref == "Neither") |>
    mutate(theta = theta * 100,
           low = low * 100,
           high = high * 100,
           shift = fct_relevel(fct_recode(factor(shift),
                              "Disability and chronic pain\n vs. neither" = "Disability and chronic pain",
                              "Disability only\n vs. neither" = "Disability only",
                              "Chronic pain only\n vs. neither" = "Chronic pain only"),
                              "Disability only\n vs. neither", "Chronic pain only\n vs. neither"),
           outcome = fct_relevel(outcome,  "OUD Hillary")) |>
    filter(shift != "Null") |>
    ggplot(aes(theta, shift, xmin = low, xmax = high)) +
    geom_linerange() +
    geom_point(size=2, shape = 15) +
    # scale_x_continuous(labels = scales::percent) +
    facet_grid(.~outcome, scales = "free_x") +
    theme_bw() +
    theme(strip.background = element_blank(),
          # strip.text = element_blank(),
          axis.text.y = element_text(size = 9),
          # strip.text = element_text(face = "bold", size=11),
          axis.text.x = element_text(size = 8),
          axis.title.x = element_text(size= 9, margin=margin(t=5)),
          text=element_text(family="Times")
          ) +
    geom_vline(xintercept = 0, linetype = "dashed") +
    labs(y = "", x = "Incidence Difference (%)") #, title= "OUD Incidence by Disability and Pain")

# free scales
p_cont <-
    contrast_tbl |>
    filter(outcome != "Any MOUD") |>
    filter(ref != "Neither") |>
    mutate(theta = theta * 100,
           low = low * 100,
           high = high * 100,
        ref = fct_recode(factor(ref),
                            "Disability and chronic pain\n vs. disability only" = "Disability only",
           "Disability and chronic pain\n vs. chronic pain only" = "Chronic pain only"),
           # ref = fct_relevel(ref, "Disability and chronic pain\n vs. pain only"),
           outcome = fct_relevel(outcome, "OUD Hillary")) |>
    filter(shift != "Null") |>
    ggplot(aes(theta, ref, xmin = low, xmax = high)) +
    geom_linerange() +
    geom_point(size=2, shape = 15) +
    # scale_x_continuous(labels = scales::percent(suffix = "")) +
    facet_grid(.~outcome, scales = "free_x") +
    theme_bw() +
    theme(strip.background = element_blank(),
          #  strip.text = element_blank(),
          # strip.text = element_text(face = "bold", size=11),
          # strip.text = element_text(face = "bold", size=11),
          axis.text.y = element_text(size = 9),
          axis.text.x = element_text(size = 8),
          axis.title.x = element_text(size= 9, margin=margin(t=5)),
          text=element_text(family="Times")
          ) +
    geom_vline(xintercept = 0, linetype = "dashed") +
    labs(y = "", x = "Incidence Difference (%)") #, title= "OUD Incidence by Disability and Pain")

p_est + p_neither + p_cont +
    plot_layout(ncol = 1 ) + #,
                #heights = c(4,3,2)) +
    plot_annotation(tag_levels = "A")
# 
# ps <- data.frame(psi_oud_hillary$g)
# colnames(ps) <- c("both", "pain", "dis", "neither")
# summary(ps) 
# 
# ps <- ps |> 
#     mutate(gsum = as.numeric(both + pain + dis + neither))
# ps |>
#     filter(gsum != 1) |> 
#     nrow()
# ps |>
#     filter(neither < 0) |> 
#     nrow()
# nrow(ps)
# ps

# ggsave(here::here("projects/create_cohort/figures/fig3_draft2.pdf"), height=7.5, width=7.5)

