####################################################
# Code author: Nick Williams
# note: this code relies on R package 'mlr3superlearner' (https://github.com/nt-williams/mlr3superlearner/tree/devel)
####################################################

tmle_mlr3 <- function(data, trt, covar, outcome,
                      g_learners = "glm", Q_learners = "glm", c_learners = "glm",
                      outcome_type = c("binomial", "continuous"), super_efficient = F,
                      .mlr3superlearner_folds = 10) {
  require("mlr3superlearner")

  folds <- 1
  folded <- origami::make_folds(data, V = folds)
  if (folds == 1) {
    folded[[1]]$training_set <- folded[[1]]$validation_set
  }

  obs <- !is.na(data[[outcome]])

  if (match.arg(outcome_type) == "continuous") {
    bounds <- c(min(data[[outcome]], na.rm = T), max(data[[outcome]], na.rm = T))
    data[[outcome]] <- (data[[outcome]] - bounds[1]) / (bounds[2] - bounds[1])
  }

  trt_factor <- as.data.table(model.matrix(~ factor(data[[trt]]) - 1))
  lvls <- levels(data[[trt]])
  names(trt_factor) <- paste0(trt, ".", lvls)

  for (i in 1:folds) {
    train <- as.data.table(data[folded[[i]]$training_set, ])
    train_trt_factor <- as.data.table(trt_factor[folded[[i]]$training_set, ])
    valid <- as.data.table(data[folded[[i]]$validation_set, ])
    valid_trt_factor <- as.data.table(trt_factor[folded[[i]]$validation_set, ])

    valids <- vector("list", length(lvls) + 1)
    names(valids) <- c("A", lvls)
    valids[["A"]] <- valid_trt_factor
    for (lvl in lvls) {
      current <- paste0(trt, ".", lvl)
      other <- setdiff(paste0(trt, ".", lvls), current)

      valids[[lvl]] <- data.table::copy(valid_trt_factor)
      valids[[lvl]][[current]] <- 1
      valids[[lvl]][, (other) := lapply(.SD, function(x) rep(0, length(x))), .SDcols = other]
    }

    Qs <- mlr3superlearner::mlr3superlearner(
      cbind(train[, c(..covar, ..outcome)], train_trt_factor)[obs, ],
      outcome,
      Q_learners,
      "glm",
      match.arg(outcome_type),
      .mlr3superlearner_folds,
      lapply(valids, function(x) cbind(valid[, ..covar], x))
    )$preds
    
    Qs <- lapply(Qs, function(x) pmax(pmin(x, 1 - 0.0001), 0.0001))

    Cs <- matrix(nrow = nrow(data), ncol = 1, data = 1)
    prob_observed <- matrix(nrow = nrow(data), ncol = 1, data = 1)
    if (!all(obs)) {
      prob_observed[folded[[i]]$validation_set, 1] <-
        mlr3superlearner::mlr3superlearner(
          cbind(train[, ..covar],
                data.table::data.table(tmp_cens = as.numeric(obs))[folded[[i]]$training_set, ]),
          "tmp_cens",
          c_learners,
          "glm",
          "binomial",
          .mlr3superlearner_folds,
          list(valid[, ..covar])
        )$preds[[1]]

      Cs[folded[[i]]$validation_set, 1] <-
        as.numeric(obs[folded[[i]]$validation_set]) / prob_observed[folded[[i]]$validation_set, 1]
    }

    g <- matrix(nrow = nrow(data), ncol = length(lvls))
    Hs <- matrix(nrow = nrow(data), ncol = length(lvls) + 1)
    colnames(Hs) <- c("A", lvls)
    colnames(g) <- c(lvls)
    for (lvl in lvls[1:(length(lvls) - 1)]) {
      target <- paste0(trt, ".", lvl)

      if (super_efficient) {
          g[folded[[i]]$validation_set, lvl] <-
              mlr3superlearner::mlr3superlearner(
                  cbind(Qs[[lvl]][folded[[i]]$training_set], 
                        train_trt_factor[, ..target]),
                  target,
                  g_learners,
                  "glm",
                  "binomial",
                  .mlr3superlearner_folds,
                  list(as.data.table(Qs[[lvl]][folded[[i]]$validation_set]))
              )$preds[[1]]
      }
      
      if (!super_efficient) {
          g[folded[[i]]$validation_set, lvl] <-
              mlr3superlearner::mlr3superlearner(
                  cbind(train[, ..covar], train_trt_factor[, ..target]),
                  target,
                  g_learners,
                  "glm",
                  "binomial",
                  .mlr3superlearner_folds,
                  list(valid[, ..covar])
              )$preds[[1]]
      }

      Hs[folded[[i]]$validation_set, lvl] <-
        valid_trt_factor[[target]] / g[folded[[i]]$validation_set, lvl]
    }

    g[folded[[i]]$validation_set, lvls[length(lvls)]] <- 1 - rowSums(g, na.rm = T)
    Hs[folded[[i]]$validation_set, lvls[length(lvls)]] <-
      valid_trt_factor[[paste0(trt, ".", lvls[length(lvls)])]] / g[folded[[i]]$validation_set, lvls[length(lvls)]]

    Hs[, "A"] <- purrr::imap_dbl(data[[trt]], function(x, i) Hs[i, as.character(x)])

    # calculate tmle
    Qeps <- matrix(nrow = nrow(data), ncol = length(lvls) + 1)
    colnames(Qeps) <- c("A", lvls)

    tmle_data <- data.frame(y = train[[outcome]],
                            Q_A = Qs[["A"]],
                            H_A = Hs[, "A"]*Cs[, 1])

    fluc <- glm(y ~ -1 + offset(qlogis(Q_A)) + H_A, data = tmle_data[obs, ], family = binomial)
    eps <- coef(fluc)

    for (lvl in c("A", lvls)) {
      Qeps[folded[[i]]$validation_set, lvl] <- plogis(qlogis(Qs[[lvl]]) + eps*Hs[, lvl]*Cs[, 1])
    }
  }

  y <- ifelse(is.na(data[[outcome]]), -999, data[[outcome]])
  psis <- apply(Qeps, 2, mean)
  eics <- lapply(c("A", lvls), function(lvl) Hs[, lvl] * Cs[, 1] * (y - Qeps[, lvl]) + Qeps[, lvl] - psis[lvl])
  names(eics) <- c("A", lvls)

  if (match.arg(outcome_type) == "continuous") {
    rescale_y_continuous <- function(scaled) {
      (scaled*(bounds[2] - bounds[1])) + bounds[1]
    }

    eics <- lapply(eics, rescale_y_continuous)
    psis <- sapply(psis, rescale_y_continuous)
  }

  ses <- lapply(c("A", lvls), function(lvl) sqrt(var(eics[[lvl]]) / nrow(data)))
  names(ses) <- c("A", lvls)

  list(psi = psis,
       std.error = ses,
       ic = eics,
       g = g,
       prob_observed = prob_observed)
}
