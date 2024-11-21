# Load necessary libraries
library(MASS)
library(brms)      # Bayesian regression
library(rstanarm)  # Bayesian tools
library(dplyr)
library(Matrix)
library(nlme)      # Mixed effects models
library(lme4)      # Extensions for mixed effects models
library(zoo)       # For filling NA values

# Enhanced logging function
log_message <- function(msg, data = NULL, level = "INFO") {
  timestamp <- format(Sys.time(), "%Y-%m-%d %H:%M:%S")
  prefix <- switch(level,
                   "INFO" = "",
                   "WARNING" = "Warning: ",
                   "ERROR" = "Error: ",
                   "DEBUG" = "Debug: ")
  
  formatted_msg <- paste0("[", timestamp, "] ", prefix, msg)
  message(formatted_msg)
  
  if (!is.null(data)) {
    if (is.data.frame(data)) {
      print(head(data))
      if (nrow(data) > 6) {
        message("... Showing only the first 6 rows, total rows: ", nrow(data))
      }
    } else {
      print(data)
    }
  }
  
  invisible(formatted_msg)
}

# Enhanced data diagnosis function
diagnose_data <- function(data, name = "") {
  log_message(paste("Diagnosing data:", name))
  
  # Basic statistics
  log_message(paste("Dimensions:", nrow(data), "x", ncol(data)))
  log_message("Data structure:")
  str(data)
  log_message("First 5 rows of data:")
  print(head(data))
  
  # NA value analysis
  na_stats <- colSums(is.na(data))
  log_message("NA value statistics:")
  print(na_stats)
  
  # Numerical column statistics
  numeric_data <- data[sapply(data, is.numeric)]
  if(ncol(numeric_data) > 0) {
    log_message("Numerical column statistics:")
    stats <- summary(numeric_data)
    print(stats)
  }
  
  # Grouped statistics
  if("Ticker" %in% colnames(data)) {
    log_message("Record count by Ticker:")
    ticker_counts <- table(data$Ticker)
    print(ticker_counts)
    
    # Check continuity of time for each Ticker
    if("Date" %in% colnames(data)) {
      ticker_gaps <- data %>%
        group_by(Ticker) %>%
        arrange(Date) %>%
        summarise(
          gaps = sum(diff(as.numeric(Date)) > 1),
          max_gap = max(diff(as.numeric(Date)))
        )
      log_message("Time series continuity check:", ticker_gaps)
    }
  }
}

# Enhanced data preprocessing function - Adding NA value handling
prepare_model_data <- function(data, train_period = NULL) {
  log_message("Starting data preprocessing and grouping...")
  
  # Initial data diagnosis
  diagnose_data(data, "Raw data")
  
  # Time period processing
  if(!is.null(train_period)) {
    data <- data %>%
      filter(Date >= train_period[1] & Date <= train_period[2])
    log_message("Filtered time period:", train_period)
  }
  
  # Handle NA values
  data <- data %>%
    group_by(Ticker) %>%
    arrange(Date) %>%
    mutate(across(where(is.numeric),
                  ~na.locf(na.locf(., na.rm = FALSE), fromLast = TRUE))) %>%
    ungroup()
  
  # Distinguish stocks and indices data
  stocks <- data %>%
    filter(Ticker %in% c("AAPL", "XOM", "NVDA", "TSLA"))
  indices <- data %>%
    filter(Ticker %in% c("SP500", "NASDAQ"))
  
  # Check group results
  diagnose_data(stocks, "Stock data")
  diagnose_data(indices, "Index data")
  
  # Improved index matching logic
  tech_stocks <- c("AAPL", "NVDA", "TSLA")
  
  # Process stock data first
  merged_data <- stocks %>%
    mutate(
      Index_Type = ifelse(Ticker %in% tech_stocks, "NASDAQ", "SP500"),
      Stock_Type = ifelse(Ticker %in% tech_stocks, "Tech", "NonTech")
    )
  
  # Reorganize index data
  index_data <- indices %>%
    select(Date, Ticker, Close) %>%
    group_by(Date) %>%
    tidyr::pivot_wider(
      names_from = Ticker,
      values_from = Close
    ) %>%
    ungroup()
  
  # Check column names of index data
  log_message("Index data column names:", names(index_data))
  
  # Merge data and handle NA values
  merged_data <- merged_data %>%
    left_join(index_data, by = "Date") %>%
    mutate(
      Index_Close = case_when(
        Index_Type == "NASDAQ" ~ .data[["NASDAQ"]],
        Index_Type == "SP500" ~ .data[["SP500"]],
        TRUE ~ NA_real_
      )
    ) %>%
    group_by(Ticker) %>%
    arrange(Date) %>%
    mutate(Index_Close = na.locf(na.locf(Index_Close, na.rm = FALSE), fromLast = TRUE)) %>%
    ungroup()
  
  # Standardization processing
  numeric_cols <- c("Close", "MA5", "MA20", "RSI", "Volatility")
  
  # Grouped standardization - Adding error handling
  merged_data <- merged_data %>%
    group_by(Ticker) %>%
    mutate(across(all_of(numeric_cols), function(x) {
      if(all(is.na(x))) return(x)
      if(sd(x, na.rm = TRUE) == 0) return(x - mean(x, na.rm = TRUE))
      as.vector(scale(x))
    })) %>%
    ungroup()
  
  # Index data standardization
  for(idx in c("NASDAQ", "SP500", "Index_Close")) {
    if(idx %in% colnames(merged_data)) {
      merged_data <- merged_data %>%
        mutate(across(idx, function(x) {
          if(all(is.na(x))) return(x)
          if(sd(x, na.rm = TRUE) == 0) return(x - mean(x, na.rm = TRUE))
          as.vector(scale(x))
        }))
    }
  }
  
  # Final data check
  diagnose_data(merged_data, "Standardized data")
  
  return(list(
    stocks = stocks,
    indices = indices,
    merged = merged_data,
    processing_info = list(
      date_range = range(data$Date),
      n_stocks = length(unique(stocks$Ticker)),
      n_indices = length(unique(indices$Ticker)),
      missing_values = colSums(is.na(merged_data)),
      standardized_columns = numeric_cols
    )
  ))
}

# Enhanced batch processing function
batch_processor <- function(data, func, batch_size = 1000, min_batch = 100) {
  log_message(paste("Starting batch processing, batch size:", batch_size))
  
  # Dynamic batch size adjustment
  n <- nrow(data)
  if(n < min_batch) {
    log_message("Data volume is too small, batch processing will not be performed", level = "WARNING")
    return(func(data))
  }
  
  # Intelligent batch division
  optimal_batch_size <- min(batch_size, max(min_batch, ceiling(n/5)))
  batches <- split(1:n, ceiling((1:n)/optimal_batch_size))
  
  # Store results
  results <- vector("list", length(batches))
  
  # Diagnose the first batch
  first_batch <- data[batches[[1]],]
  diagnose_data(first_batch, "First batch")
  
  # Batch processing
  for(i in seq_along(batches)) {
    log_message(paste("Processing batch", i, "/", length(batches)))
    batch_data <- data[batches[[i]],]
    
    results[[i]] <- tryCatch({
      res <- func(batch_data)
      if(i == 1) {
        log_message("First batch result type:", class(res))
        log_message("First batch result structure:")
        str(res)
      }
      res
    }, error = function(e) {
      log_message(paste("Batch processing error:", e$message), level = "ERROR")
      log_message("Problematic batch data overview:")
      diagnose_data(batch_data, paste("Error batch", i))
      NULL
    })
    
    # Memory management
    if(i %% 3 == 0) gc()
  }
  
  # Result verification and merging
  valid_results <- Filter(Negate(is.null), results)
  
  
  if(length(valid_results) == 0) {
    stop("All batches failed to process")
  }
  
  combine_results(valid_results)
}

# Improved result combination function - handling more model types
combine_results <- function(results) {
  log_message("Combining results...")
  log_message("Result type:", class(results[[1]]))
  
  if(length(results) == 0) return(NULL)
  
  tryCatch({
    if(is.list(results[[1]]) && all(sapply(results[[1]], inherits, what = "lm"))) {
      # Handle multiple linear models
      log_message("Combining multiple linear model results")
      combined_models <- list()
      for(model_name in names(results[[1]])) {
        combined_models[[model_name]] <- results[[1]][[model_name]]
      }
      return(combined_models)
    }
    else if(inherits(results[[1]], "lm") ||
            inherits(results[[1]], "glm") ||
            inherits(results[[1]], "rlm")) {
      log_message("Combining single regression model result")
      return(results[[1]])
    }
    else if(inherits(results[[1]], "lme")) {
      log_message("Combining mixed-effects model result")
      return(results[[1]])
    }
    else if(inherits(results[[1]], "brmsfit")) {
      log_message("Combining Bayesian model result")
      return(results[[1]])
    }
    else if(is.list(results[[1]]) && "coefficients" %in% names(results[[1]])) {
      log_message("Combining coefficient results")
      log_message("Coefficients of the first result:")
      print(results[[1]]$coefficients)
      
      # Weighted average combination
      n_obs <- sapply(results, function(x) length(residuals(x)))
      weights <- n_obs / sum(n_obs)
      
      # Calculate weighted average coefficients
      all_coeffs <- do.call(rbind, lapply(seq_along(results), function(i) {
        results[[i]]$coefficients * weights[i]
      }))
      combined_coeffs <- colSums(all_coeffs)
      
      # Calculate combined standard errors
      var_coeffs <- do.call(rbind, lapply(seq_along(results), function(i) {
        vcov(results[[i]]) * weights[i]^2
      }))
      combined_se <- sqrt(colSums(var_coeffs))
      
      return(list(
        coefficients = combined_coeffs,
        std.error = combined_se,
        n_models = length(results),
        total_obs = sum(n_obs)
      ))
    }
    
    log_message("Using default combination strategy")
    results[[1]]
    
  }, error = function(e) {
    log_message(paste("Error combining results:", e$message), level = "ERROR")
    log_message("Result structure:")
    str(results)
    NULL
  })
}

# Model prediction function - Improved handling of multiple model predictions
predict_models <- function(models, newdata) {
  if(is.list(models) && !inherits(models, c("brmsfit", "lme", "lm", "glm", "rlm"))) {
    # Handle multiple models
    predictions <- list()
    for(model_name in names(models)) {
      if(!is.null(models[[model_name]])) {
        tryCatch({
          pred <- predict_single_model(models[[model_name]], newdata)
          if(!is.null(pred)) predictions[[model_name]] <- pred
        }, error = function(e) {
          log_message(paste("Prediction failed:", model_name, "-", e$message),
                      level = "ERROR")
        })
      }
    }
    
    if(length(predictions) == 0) {
      return(NULL)
    }
    
    # Calculate average prediction
    pred_matrix <- do.call(cbind, predictions)
    return(rowMeans(pred_matrix, na.rm = TRUE))
  } else {
    return(predict_single_model(models, newdata))
  }
}

# Single model prediction function
predict_single_model <- function(model, newdata) {
  if(is.null(model)) return(NULL)
  
  tryCatch({
    if(inherits(model, "brmsfit")) {
      predict(model, newdata = newdata, summary = TRUE)[,"Estimate"]
    } else if(inherits(model, "lme")) {
      predict(model, newdata = newdata, level = 0)
    } else if(inherits(model, "lm") || inherits(model, "glm") ||
              inherits(model, "rlm")) {
      predict(model, newdata = newdata)
    } else {
      stop("Unsupported model type")
    }
  }, error = function(e) {
    log_message(paste("Prediction failed:", e$message), level = "ERROR")
    NULL
  })
}

# Improved No Pooling model function
no_pooling_model <- function(data) {
  log_message("Executing No Pooling model...")
  
  stocks_data <- data$stocks
  diagnose_data(stocks_data, "No Pooling input data")
  
  results <- list()
  
  for(stock in unique(stocks_data$Ticker)) {
    log_message(paste("Processing stock:", stock))
    stock_data <- stocks_data[stocks_data$Ticker == stock,]
    
    if(nrow(stock_data) == 0) {
      log_message(paste("Warning: No data for", stock), level = "WARNING")
      next
    }
    
    if(any(is.na(stock_data$Close)) || any(is.na(stock_data$MA5)) ||
       any(is.na(stock_data$MA20)) || any(is.na(stock_data$RSI)) ||
       any(is.na(stock_data$Volatility))) {
      log_message(paste("Warning:", stock, "contains NA values, filling them"), level = "WARNING")
      stock_data <- stock_data %>%
        arrange(Date) %>%
        mutate(across(c(Close, MA5, MA20, RSI, Volatility),
                      ~na.locf(na.locf(., na.rm = FALSE), fromLast = TRUE)))
    }
    
    # Linear regression
    tryCatch({
      results[[paste0(stock, "_linear")]] <- batch_processor(stock_data, function(d) {
        model <- lm(Close ~ MA5 + MA20 + RSI + Volatility, data = d)
        log_message(paste("Linear regression model summary -", stock))
        print(summary(model))
        model
      })
    }, error = function(e) {
      log_message(paste("Linear regression failed:", stock, "-", e$message), level = "ERROR")
    })
    
    # Bayesian regression
    tryCatch({
      results[[paste0(stock, "_bayes")]] <- batch_processor(stock_data, function(d) {
        prior_settings <- c(
          set_prior("normal(0, 10)", class = "b"),
          set_prior("student_t(3, 0, 10)", class = "Intercept")
        )
        
        model <- brm(
          Close ~ MA5 + MA20 + RSI + Volatility,
          data = d,
          family = gaussian(),
          prior = prior_settings,
          chains = 2,
          iter = 2000,
          warmup = 1000,
          cores = 1,
          refresh = 0  # Suppress progress output
        )
        
        log_message(paste("Bayesian model summary -", stock))
        print(summary(model))
        model
      })
    }, error = function(e) {
      log_message(paste("Bayesian regression failed:", stock, "-", e$message), level = "ERROR")
    })
    
    # GLM
    tryCatch({
      results[[paste0(stock, "_glm")]] <- batch_processor(stock_data, function(d) {
        model <- glm(Close ~ MA5 + MA20 + RSI + Volatility,
                     family = gaussian(link = "identity"),
                     data = d)
        log_message(paste("GLM model summary -", stock))
        print(summary(model))
        model
      })
    }, error = function(e) {
      log_message(paste("GLM failed:", stock, "-", e$message), level = "ERROR")
    })
    
    gc()
  }
  
  log_message("No Pooling model completed")
  log_message("Overview of results:")
  print(names(results))
  
  return(results)
}

# Improved Partial Pooling model function
partial_pooling_model <- function(data) {
  log_message("Executing Partial Pooling model...")
  
  merged_data <- data$merged
  
  if(any(is.na(merged_data$Close)) || any(is.na(merged_data$Index_Close))) {
    log_message("Warning: NA values found in data, filling them", level = "WARNING")
    merged_data <- merged_data %>%
      group_by(Ticker) %>%
      arrange(Date) %>%
      mutate(across(c(Close, Index_Close, MA5, MA20, RSI, Volatility),
                    ~na.locf(na.locf(., na.rm = FALSE), fromLast = TRUE))) %>%
      ungroup()
  }
  
  diagnose_data(merged_data, "Partial Pooling input data")
  
  results <- list()
  
  # Module 1: Hierarchical Bayesian model
  tryCatch({
    results$hierarchical_bayes <- batch_processor(merged_data, function(d) {
      prior_settings <- c(
        set_prior("normal(0, 2)", class = "b"),
        set_prior("student_t(3, 0, 2)", class = "Intercept"),
        set_prior("cauchy(0, 1)", class = "sd", group = "Ticker"),
        set_prior("cauchy(0, 1)", class = "sd", group = "Index_Type")
      )
      
      model <- brm(
        Close ~ MA5 + MA20 + RSI + Volatility + Index_Close + (1|Index_Type/Ticker),
        data = d,
        family = gaussian(),
        prior = prior_settings,
        chains = 4,
        iter = 4000,
        warmup = 2000,
        cores = 1,
        control = list(adapt_delta = 0.95),
        refresh = 0  # Suppress progress output
      )
      
      log_message("Hierarchical Bayesian model summary:")
      print(summary(model))
      
      model
    })
  }, error = function(e) {
    log_message(paste("Hierarchical Bayesian model failed:", e$message), level = "ERROR")
  })
  
  # Module 2: Mixed-effects model
  tryCatch({
    results$mixed_effects <- batch_processor(merged_data, function(d) {
      model <- lme(Close ~ MA5 + MA20 + RSI + Volatility + Index_Close,
                   random = list(Index_Type = ~ 1, Ticker = ~ 1),
                   data = d,
                   method = "REML",
                   control = lmeControl(opt = "optim",
                                        maxIter = 200,
                                        msMaxIter = 200))
      
      log_message("Mixed-effects model summary:")
      print(summary(model))
      
      model
    })
  }, error = function(e) {
    log_message(paste("Mixed-effects model failed:", e$message), level = "ERROR")
  })
  
  log_message("Partial Pooling model completed")
  log_message("Overview of results:", names(results))
  
  return(results)
}

# Improved Complete Pooling model function
complete_pooling_model <- function(data) {
  log_message("Executing Complete Pooling model...")
  
  merged_data <- data$merged
  
  if(any(is.na(merged_data$Close)) || any(is.na(merged_data$Index_Close))) {
    log_message("Warning: NA values found in data, filling them", level = "WARNING")
    merged_data <- merged_data %>%
      group_by(Ticker) %>%
      arrange(Date) %>%
      mutate(across(c(Close, Index_Close, MA5, MA20, RSI, Volatility),
                    ~na.locf(na.locf(., na.rm = FALSE), fromLast = TRUE))) %>%
      ungroup()
  }
  
  diagnose_data(merged_data, "Complete Pooling input data")
  
  results <- list()
  
  # Module 1: Robust linear regression
  tryCatch({
    results$robust_regression <- batch_processor(merged_data, function(d) {
      model <- rlm(Close ~ MA5 + MA20 + RSI + Volatility + Index_Close,
                   data = d,
                   psi = psi.huber,
                   method = "MM")
      
      log_message("Robust regression model summary:")
      print(summary(model))
      model
    })
  }, error = function(e) {
    log_message(paste("Robust regression failed:", e$message), level = "ERROR")
  })
  
  # Module 2: Bayesian pooled regression
  tryCatch({
    results$pooled_bayes <- batch_processor(merged_data, function(d) {
      prior_settings <- c(
        set_prior("normal(0, 1)", class = "b"),
        set_prior("student_t(3, 0, 1)", class = "Intercept"),
        set_prior("student_t(3, 0, 1)", class = "sigma")
      )
      
      model <- brm(
        Close ~ MA5 + MA20 + RSI + Volatility + Index_Close,
        data = d,
        family = student,
        prior = prior_settings,
        chains = 4,
        iter = 4000,
        warmup = 2000,
        cores = 1,
        refresh = 0  # Suppress progress output
      )
      
      log_message("Pooled Bayesian model summary:")
      print(summary(model))
      model
    })
  }, error = function(e) {
    log_message(paste("Pooled Bayesian regression failed:", e$message), level = "ERROR")
  })
  
  log_message("Complete Pooling model completed")
  log_message("Overview of results:", names(results))
  
  return(results)
}

# Improved evaluation function
evaluate_model <- function(models, test_data, actual = NULL) {
  log_message("Starting model evaluation...")
  log_message("Model type:", class(models))
  
  if(is.null(models)) {
    log_message("Warning: Model is NULL", level = "WARNING")
    return(list(RMSE = NA, MAE = NA, R2 = NA, MAPE = NA, DA = NA, IR = NA))
  }
  
  tryCatch({
    # Obtain predictions
    predictions <- predict_models(models, test_data)
    if(is.null(predictions)) {
      log_message("Warning: Prediction failed", level = "WARNING")
      return(list(RMSE = NA, MAE = NA, R2 = NA, MAPE = NA, DA = NA, IR = NA))
    }
    
    actual <- if(is.null(actual)) test_data$Close else actual
    
    # Handle NA values
    valid_idx <- !is.na(predictions) & !is.na(actual)
    predictions <- predictions[valid_idx]
    actual <- actual[valid_idx]
    
    if(length(predictions) == 0) {
      log_message("Warning: No valid predictions", level = "WARNING")
      return(list(RMSE = NA, MAE = NA, R2 = NA, MAPE = NA, DA = NA, IR = NA))
    }
    
    # Calculate evaluation metrics
    rmse <- sqrt(mean((predictions - actual)^2))
    mae <- mean(abs(predictions - actual))
    r2 <- 1 - sum((predictions - actual)^2) / sum((actual - mean(actual))^2)
    
    # Handle possible zero division in MAPE
    actual_nonzero <- actual != 0
    mape <- if(any(actual_nonzero)) {
      mean(abs((predictions[actual_nonzero] - actual[actual_nonzero])/
                 actual[actual_nonzero])) * 100
    } else {
      NA
    }
    
    # Direction accuracy
    direction_actual <- c(NA, diff(actual))
    direction_pred <- c(NA, diff(predictions))
    da <- mean(sign(direction_actual) == sign(direction_pred), na.rm = TRUE)
    
    # Information ratio - handle zero standard deviation
    returns_pred <- (predictions - actual) / abs(actual)
    returns_sd <- sd(returns_pred, na.rm = TRUE)
    ir <- if(!is.na(returns_sd) && returns_sd > 0) {
      mean(returns_pred, na.rm = TRUE) / returns_sd
    } else {
      NA
    }
    
    # Output results
    results <- list(
      RMSE = rmse,
      MAE = mae,
      R2 = r2,
      MAPE = mape,
      DA = da,
      IR = ir,
      n_predictions = length(predictions)
    )
    
    log_message("Evaluation metrics:")
    print(results)
    
    results
    
  }, error = function(e) {
    log_message(paste("Evaluation error:", e$message), level = "ERROR")
    list(RMSE = NA, MAE = NA, R2 = NA, MAPE = NA, DA = NA, IR = NA)
  })
}

# Improved cross-validation function
cross_validate <- function(data, k = 5, seed = 42) {
  log_message(paste("Starting", k, "-fold cross-validation"))
  
  set.seed(seed)
  
  # Ensure data is sorted by date
  data <- data %>% arrange(Date)
  
  # Get unique dates
  unique_dates <- unique(data$Date)
  n_dates <- length(unique_dates)
  fold_size <- floor(n_dates / k)
  
  # Store all results
  results <- list()
  
  for(i in 1:k) {
    log_message(paste("Processing fold", i))
    
    # Calculate time window
    if(i < k) {
      test_dates <- unique_dates[((i-1)*fold_size + 1):(i*fold_size)]
    } else {
      test_dates <- unique_dates[((i-1)*fold_size + 1):n_dates]
    }
    
    # Split data
    test_data <- data %>% filter(Date %in% test_dates)
    train_data <- data %>% filter(!(Date %in% test_dates))
    
    # Check split results
    log_message(paste("Training set size:", nrow(train_data), "rows,",
                      length(unique(train_data$Date)), "trading days"))
    log_message(paste("Test set size:", nrow(test_data), "rows,",
                      length(unique(test_data$Date)), "trading days"))
    
    # Data preprocessing
    train_processed <- prepare_model_data(train_data)
    test_processed <- prepare_model_data(test_data)
    
    # Model training and evaluation
    fold_results <- tryCatch({
      # No Pooling model
      no_pooling <- no_pooling_model(train_processed)
      
      # Partial Pooling model
      partial_pooling <- partial_pooling_model(train_processed)
      
      # Complete Pooling model
      complete_pooling <- complete_pooling_model(train_processed)
      
      # Evaluation results
      evaluation_results <- list(
        no_pooling = evaluate_model(no_pooling, test_processed$merged),
        partial_pooling = evaluate_model(partial_pooling, test_processed$merged),
        complete_pooling = evaluate_model(complete_pooling, test_processed$merged)
      )
      
      list(
        models = list(
          no_pooling = no_pooling,
          partial_pooling = partial_pooling,
          complete_pooling = complete_pooling
        ),
        evaluation = evaluation_results
      )
    }, error = function(e) {
      log_message(paste("Fold processing error:", e$message), level = "ERROR")
      NULL
    })
    
    # Save current fold results
    results[[i]] <- fold_results
    
    # Output current fold evaluation results
    if(!is.null(fold_results)) {
      log_message(paste("Fold", i, "evaluation results:"))
      print(fold_results$evaluation)
    }
    
    # Memory management
    gc()
  }
  
  # Validate results
  valid_results <- Filter(Negate(is.null), results)
  if(length(valid_results) == 0) {
    stop("All cross-validation folds failed")
  }
  
  # Summarize results
  summarize_cv_results(valid_results, k)
}

# Improved evaluation of single fold results function
evaluate_fold <- function(fold_results, test_data) {
  evaluations <- list()
  
  for(model_type in names(fold_results)) {
    if(!is.null(fold_results[[model_type]])) {
      tryCatch({
        evaluations[[model_type]] <- evaluate_model(
          fold_results[[model_type]],
          test_data
        )
      }, error = function(e) {
        log_message(paste("Evaluation failed:", model_type, "-", e$message), level = "ERROR")
        evaluations[[model_type]] <- list(
          RMSE = NA, MAE = NA, R2 = NA, MAPE = NA, DA = NA, IR = NA
        )
      })
    }
  }
  
  return(evaluations)
}

# Improved cross-validation result summary function
summarize_cv_results <- function(results, k) {
  log_message(paste("Summarizing", k, "-fold cross-validation results"))
  
  metrics <- c("RMSE", "MAE", "R2", "MAPE", "DA", "IR")
  summary <- list()
  
  # Get all model types
  model_types <- unique(unlist(lapply(results, function(x) {
    if(!is.null(x$evaluation)) names(x$evaluation)
  })))
  
  for(model_type in model_types) {
    model_metrics <- matrix(NA, nrow = k, ncol = length(metrics))
    colnames(model_metrics) <- metrics
    
    # Collect results from all folds
    for(i in seq_along(results)) {
      if(!is.null(results[[i]]$evaluation[[model_type]])) {
        model_metrics[i,] <- sapply(metrics, function(m) {
          results[[i]]$evaluation[[model_type]][[m]]
        })
      }
    }
    
    # Calculate mean and standard deviation
    summary[[model_type]] <- list(
      means = colMeans(model_metrics, na.rm = TRUE),
      sds = apply(model_metrics, 2, sd, na.rm = TRUE),
      all_folds = model_metrics
    )
  }
  
  log_message("Cross-validation result summary:")
  print(summary)
  
  return(summary)
}

# Improved report generation function
generate_report <- function(results, models = NULL) {
  log_message("Generating analysis report...")
  
  tryCatch({
    # Output file path
    output_file <- "model_analysis_report.txt"
    
    # Write report
    sink(output_file)
    
    cat("=== Financial Market Modeling Analysis Report ===\n")
    cat("Generated at:", format(Sys.time(), "%Y-%m-%d %H:%M:%S"), "\n\n")
    
    # Model performance summary
    cat("1. Model Performance Summary\n")
    cat("---------------------------\n")
    for(model_name in names(results)) {
      cat("\nModel:", model_name, "\n")
      metrics <- results[[model_name]]
      if(!is.null(metrics$means)) {
        df <- data.frame(
          Metric = names(metrics$means),
          Mean = round(metrics$means, 4),
          SD = round(metrics$sds, 4)
        )
        print(df)
      }
    }
    
    # Model comparison
    cat("\n2. Model Comparison\n")
    cat("--------------------\n")
    comparison <- do.call(rbind, lapply(results, function(x) {
      if(!is.null(x$means)) x$means else rep(NA, 6)
    }))
    rownames(comparison) <- names(results)
    print(round(comparison, 4))
    
    # If model objects are provided, add model diagnostics
    if(!is.null(models)) {
      cat("\n3. Model Diagnostics\n")
      cat("---------------------\n")
      for(model_type in names(models)) {
        if(!is.null(models[[model_type]])) {
          cat("\n", model_type, ":\n")
          model_list <- models[[model_type]]
          for(model_name in names(model_list)) {
            if(!is.null(model_list[[model_name]])) {
              cat("\n  ", model_name, ":\n")
              tryCatch({
                if(inherits(model_list[[model_name]], "brmsfit")) {
                  print(summary(model_list[[model_name]]))
                } else if(inherits(model_list[[model_name]], "lme")) {
                  print(summary(model_list[[model_name]]))
                } else {
                  print(summary(model_list[[model_name]]))
                }
              }, error = function(e) {
                cat("  Unable to generate model summary:", e$message, "\n")
              })
            }
          }
        }
      }
    }
    
    sink()
    
    log_message(paste("Report generated:", output_file))
    
  }, error = function(e) {
    log_message(paste("Report generation failed:", e$message), level = "ERROR")
  })
}

# Data alignment check function
check_data_alignment <- function(processed_data) {
  log_message("Checking data alignment...")
  
  # Training set check
  train_alignment <- check_single_dataset(processed_data$train, "Training set")
  
  # Test set check
  test_alignment <- check_single_dataset(processed_data$test, "Test set")
  
  return(list(
    train = train_alignment,
    test = test_alignment
  ))
}

# Helper function: Check alignment of a single dataset
check_single_dataset <- function(data, dataset_name) {
  if(is.null(data)) {
    stop(paste(dataset_name, "is NULL"))
  }
  
  # Check date range for each Ticker
  date_ranges <- data %>%
    group_by(Ticker) %>%
    summarise(
      min_date = min(Date),
      max_date = max(Date),
      n_dates = n_distinct(Date)
    )
  
  log_message(paste(dataset_name, "date range for each Ticker:"))
  print(date_ranges)
  
  # Check if all Tickers have the same dates
  all_dates <- data %>%
    group_by(Date) %>%
    summarise(
      n_tickers = n_distinct(Ticker),
      tickers = paste(sort(unique(Ticker)), collapse = ", ")
    ) %>%
    filter(n_tickers != n_distinct(data$Ticker))
  
  if(nrow(all_dates) > 0) {
    log_message(paste("Warning:", dataset_name, "contains incomplete dates:"), level = "WARNING")
    print(all_dates)
  }
  
  # Check data continuity
  ticker_gaps <- data %>%
    group_by(Ticker) %>%
    arrange(Date) %>%
    summarise(
      gaps = sum(diff(as.numeric(Date)) > 1),
      max_gap = max(diff(as.numeric(Date)))
    )
  
  log_message(paste(dataset_name, "data continuity check:"))
  print(ticker_gaps)
  
  return(list(
    date_ranges = date_ranges,
    incomplete_dates = all_dates,
    gaps = ticker_gaps
  ))
}

# Helper function: Check data quality
check_data_quality <- function(data, dataset_name) {
  log_message(paste("Checking", dataset_name, "data quality..."))
  
  # Check required columns
  required_cols <- c("Date", "Ticker", "Close", "High", "Low", "Open",
                     "Volume", "Returns", "RSI", "MA5", "MA20", "Volatility")
  missing_cols <- setdiff(required_cols, colnames(data))
  
  if(length(missing_cols) > 0) {
    log_message(paste("Warning:", dataset_name, "is missing columns:",
                      paste(missing_cols, collapse = ", ")), level = "WARNING")
  }
  
  # Check NA values
  na_counts <- colSums(is.na(data))
  if(any(na_counts > 0)) {
    log_message(paste(dataset_name, "NA count:"))
    print(na_counts[na_counts > 0])
  }
  
  # Check outliers
  numeric_cols <- names(data)[sapply(data, is.numeric)]
  outliers <- lapply(numeric_cols, function(col) {
    if(!all(is.na(data[[col]]))) {
      vals <- data[[col]]
      q1 <- quantile(vals, 0.25, na.rm = TRUE)
      q3 <- quantile(vals, 0.75, na.rm = TRUE)
      iqr <- q3 - q1
      lower <- q1 - 1.5 * iqr
      upper <- q3 + 1.5 * iqr
      n_outliers <- sum(vals < lower | vals > upper, na.rm = TRUE)
      return(n_outliers)
    }
    return(0)
  })
  names(outliers) <- numeric_cols
  
  if(any(unlist(outliers) > 0)) {
    log_message(paste(dataset_name, "outlier count:"))
    print(outliers[outliers > 0])
  }
  
  return(list(
    missing_columns = missing_cols,
    na_counts = na_counts,
    outliers = outliers
  ))
}

# Main function
main <- function() {
  log_message("Starting main program execution...")
  
  tryCatch({
    # Read preprocessed data
    log_message("Reading data...")
    processed_data <- readRDS("processed_market_data.rds")
    
    if(is.null(processed_data)) {
      stop("processed_market_data.rds is NULL")
    }
    
    # Check data integrity
    log_message("Checking data integrity...")
    alignment_check <- check_data_alignment(processed_data)
    
    # If data is incomplete, fix it
    if(nrow(alignment_check$train$incomplete_dates) > 0 ||
       nrow(alignment_check$test$incomplete_dates) > 0) {
      log_message("Fixing data alignment issues...")
      
      # Training set processing
      train_dates <- seq(
        min(processed_data$train$Date),
        max(processed_data$train$Date),
        by = "day"
      )
      fixed_train <- expand.grid(
        Date = train_dates,
        Ticker = unique(processed_data$train$Ticker),
        stringsAsFactors = FALSE
      ) %>%
        as_tibble() %>%
        left_join(processed_data$train, by = c("Date", "Ticker"))
      
      # Test set processing
      test_dates <- seq(
        min(processed_data$test$Date),
        max(processed_data$test$Date),
        by = "day"
      )
      fixed_test <- expand.grid(
        Date = test_dates,
        Ticker = unique(processed_data$test$Ticker),
        stringsAsFactors = FALSE
      ) %>%
        as_tibble() %>%
        left_join(processed_data$test, by = c("Date", "Ticker"))
      
      # Fill NA values
      fixed_train <- fixed_train %>%
        group_by(Ticker) %>%
        arrange(Date) %>%
        mutate(across(where(is.numeric),
                      ~na.locf(na.locf(., na.rm = FALSE), fromLast = TRUE))) %>%
        ungroup()
      
      fixed_test <- fixed_test %>%
        group_by(Ticker) %>%
        arrange(Date) %>%
        mutate(across(where(is.numeric),
                      ~na.locf(na.locf(., na.rm = FALSE), fromLast = TRUE))) %>%
        ungroup()
      
      log_message("Data fix completed")
      log_message(paste("Training set: Original rows:", nrow(processed_data$train),
                        "Fixed rows:", nrow(fixed_train)))
      log_message(paste("Test set: Original rows:", nrow(processed_data$test),
                        "Fixed rows:", nrow(fixed_test)))
      
      processed_data$train <- fixed_train
      processed_data$test <- fixed_test
    }
    
    # Use training set data
    data <- processed_data$train
    
    if(is.null(data)) {
      stop("Training set is NULL")
    }
    
    # Check data structure
    required_cols <- c("Date", "Ticker", "Close", "High", "Low", "Open",
                       "Volume", "Returns", "RSI", "MA5", "MA20", "Volatility")
    missing_cols <- setdiff(required_cols, colnames(data))
    if(length(missing_cols) > 0) {
      log_message(paste("Current column names:", paste(colnames(data), collapse = ", ")))
      stop(paste("Data is missing required columns:", paste(missing_cols, collapse = ", ")))
    }
    
    # Check stock codes
    required_tickers <- c("AAPL", "XOM", "NVDA", "TSLA", "SP500", "NASDAQ")
    missing_tickers <- setdiff(required_tickers, unique(data$Ticker))
    if(length(missing_tickers) > 0) {
      stop(paste("Data is missing required stock codes:", paste(missing_tickers, collapse = ", ")))
    }
    
    log_message("Data loaded successfully, starting modeling...")
    log_message(paste("Data dimensions:", nrow(data), "x", ncol(data)))
    log_message(paste("Date range:", min(data$Date), "to", max(data$Date)))
    log_message("Stock count statistics:")
    print(table(data$Ticker))
    
    # Execute cross-validation
    log_message("Starting cross-validation...")
    cv_results <- cross_validate(data, k = 5)
    
    # Train final model on full dataset
    log_message("Training final model on full dataset...")
    model_data <- prepare_model_data(data)
    
    # Train models
    log_message("Starting training for each model type...")
    models <- list()
    
    # No Pooling
    log_message("Training No Pooling model...")
    models$no_pooling <- tryCatch({
      no_pooling_model(model_data)
    }, error = function(e) {
      log_message(paste("No Pooling model failed:", e$message), level = "ERROR")
      NULL
    })
    
    # Partial Pooling
    log_message("Training Partial Pooling model...")
    models$partial_pooling <- tryCatch({
      partial_pooling_model(model_data)
    }, error = function(e) {
      log_message(paste("Partial Pooling model failed:", e$message), level = "ERROR")
      NULL
    })
    
    # Complete Pooling
    log_message("Training Complete Pooling model...")
    models$complete_pooling <- tryCatch({
      complete_pooling_model(model_data)
    }, error = function(e) {
      log_message(paste("Complete Pooling model failed:", e$message), level = "ERROR")
      NULL
    })
    
    # Evaluate test set performance
    log_message("Evaluating test set performance...")
    test_data <- processed_data$test
    if(!is.null(test_data)) {
      test_results <- list()
      test_data_processed <- prepare_model_data(test_data)
      
      for(model_type in names(models)) {
        if(!is.null(models[[model_type]])) {
          test_results[[model_type]] <- tryCatch({
            evaluate_model(models[[model_type]], test_data_processed$merged)
          }, error = function(e) {
            log_message(paste("Test set evaluation failed:", model_type, "-", e$message),
                        level = "ERROR")
            NULL
          })
        }
      }
      
      log_message("Test set evaluation results:")
      print(test_results)
    } else {
      log_message("Warning: No test set data", level = "WARNING")
    }
    
    # Generate final report
    log_message("Generating analysis report...")
    generate_report(cv_results, models)
    
    # Save results
    log_message("Saving models and results...")
    final_results <- list(
      models = models,
      cv_results = cv_results,
      test_results = if(exists("test_results")) test_results else NULL,
      processing_info = list(
        training_dates = range(data$Date),
        n_observations = nrow(data),
        n_stocks = length(unique(data$Ticker[data$Ticker %in%
                                               c("AAPL", "XOM", "NVDA", "TSLA")])),
        n_indices = length(unique(data$Ticker[data$Ticker %in%
                                                c("SP500", "NASDAQ")]))
      )
    )
    
    saveRDS(final_results, "final_model_results.rds")
    
    log_message("Program execution completed!")
    return(final_results)
    
  }, error = function(e) {
    log_message(paste("Program execution failed:", e$message), level = "ERROR")
    return(NULL)
  })
}

# Run program
if(sys.nframe() == 0) {
  # Set number of parallel computing cores
  options(mc.cores = parallel::detectCores()-1)
  
  # Set random seed
  set.seed(42)
  
  # Start main program
  results <- main()
  
  # Check results
  if(!is.null(results)) {
    cat("\nProgram executed successfully!\n")
    cat("Model types:", names(results$models), "\n")
    cat("Cross-validation count:", length(results$cv_results), "\n")
  } else {
    cat("\nProgram execution failed!\n")
  }
}
