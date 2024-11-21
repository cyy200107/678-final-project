# Load necessary libraries
library(MASS)      
library(brms)      
library(rstanarm)  
library(dplyr)     

# File paths configuration
file_paths <- list(
  AAPL = "AAPL_stock_data.csv",
  XOM = "XOM_5_years_data.csv",
  NVDA = "NVDA_5_years_data.csv",
  TSLA = "TSLA_5_years_data.csv",
  SP500 = "SP500_data.csv",
  NASDAQ = "NASDAQ_Composite_data.csv"
)

# Logging function
log_message <- function(msg) {
  timestamp <- format(Sys.time(), "%Y-%m-%d %H:%M:%S")
  message(paste0("[", timestamp, "] ", msg))
  flush.console()
}

# Moving average calculation function - using basic operations
calculate_ma <- function(x, window) {
  n <- length(x)
  result <- vector("numeric", n)
  result[1:(window-1)] <- NA
  for(i in window:n) {
    result[i] <- mean(x[(i-window+1):i], na.rm = TRUE)
  }
  return(result)
}

# Volatility calculation function - using basic operations
calculate_volatility <- function(returns, window = 20) {
  n <- length(returns)
  result <- vector("numeric", n)
  result[1:(window-1)] <- NA
  for(i in window:n) {
    result[i] <- sd(returns[(i-window+1):i], na.rm = TRUE)
  }
  return(result)
}

# Data loading function
load_stock_data <- function(file_path, ticker) {
  log_message(paste("Loading data for", ticker))
  
  data <- tryCatch({
    df <- read.csv(file_path)
    df$Ticker <- ticker
    df$Date <- as.Date(gsub(" .*$", "", df$Date))
    df[, c("Date", "Ticker", "Close", "High", "Low", "Open", "Volume")]
  }, error = function(e) {
    log_message(paste("Error: Unable to load data for", ticker, "-", e$message))
    return(NULL)
  })
  
  if(!is.null(data)) {
    log_message(paste("Data loaded for", ticker, "with rows:", nrow(data)))
  }
  return(data)
}

# RSI calculation function - using basic operations
calculate_rsi <- function(prices, window = 14) {
  diffs <- diff(prices)
  gains <- pmax(0, diffs)
  losses <- abs(pmin(0, diffs))
  
  result <- rep(NA, length(prices))
  
  for(i in (window+1):length(prices)) {
    avg_gain <- mean(gains[(i-window):(i-1)], na.rm = TRUE)
    avg_loss <- mean(losses[(i-window):(i-1)], na.rm = TRUE)
    rs <- avg_gain / (avg_loss + 1e-10)  # Avoid division by zero
    result[i] <- 100 * (1 - 1/(1 + rs))
  }
  
  return(result)
}

# Feature calculation function
calculate_features <- function(data) {
  # Sort by date
  data <- data[order(data$Date), ]
  
  # Initialize result
  result <- data
  result$Returns <- NA
  result$RSI <- NA
  result$MA5 <- NA
  result$MA20 <- NA
  result$Volatility <- NA
  
  # Compute all indicators by Ticker
  for(tick in unique(data$Ticker)) {
    idx <- which(data$Ticker == tick)
    prices <- data$Close[idx]
    
    # Calculate returns
    result$Returns[idx] <- c(NA, diff(prices)/prices[-length(prices)])
    
    # Calculate RSI
    result$RSI[idx] <- calculate_rsi(prices)
    
    # Calculate moving averages
    result$MA5[idx] <- calculate_ma(prices, 5)
    result$MA20[idx] <- calculate_ma(prices, 20)
    
    # Calculate volatility
    result$Volatility[idx] <- calculate_volatility(result$Returns[idx])
  }
  
  return(result)
}

# Main preprocessing function
preprocess_market_data <- function() {
  log_message("Starting data preprocessing...")
  
  # Load all data
  all_data <- list()
  for(ticker in names(file_paths)) {
    data <- load_stock_data(file_paths[[ticker]], ticker)
    if(!is.null(data)) {
      all_data[[ticker]] <- data
    }
  }
  
  # Combine data
  combined_data <- do.call(rbind, all_data)
  log_message(paste("Data combined, total rows:", nrow(combined_data)))
  
  # Calculate features
  processed_data <- calculate_features(combined_data)
  log_message("Feature calculation completed")
  
  # Remove rows with NA
  processed_data <- na.omit(processed_data)
  log_message(paste("Data cleaned, remaining rows:", nrow(processed_data)))
  
  # Separate stock and index data
  stocks_data <- processed_data[processed_data$Ticker %in% c("AAPL", "XOM", "NVDA", "TSLA"), ]
  indices_data <- processed_data[processed_data$Ticker %in% c("SP500", "NASDAQ"), ]
  
  # Split data
  total_dates <- sort(unique(processed_data$Date))
  split_idx <- floor(length(total_dates) * 0.8)
  split_date <- total_dates[split_idx]
  
  # Prepare training and testing datasets
  train_data <- processed_data[processed_data$Date <= split_date, ]
  test_data <- processed_data[processed_data$Date > split_date, ]
  
  # Standardization
  numeric_cols <- c("Close", "Returns", "RSI", "MA5", "MA20", "Volatility")
  
  # Standardize using basic R operations
  for(col in numeric_cols) {
    for(tick in unique(processed_data$Ticker)) {
      # Training data
      train_idx <- train_data$Ticker == tick
      train_mean <- mean(train_data[train_idx, col], na.rm = TRUE)
      train_sd <- sd(train_data[train_idx, col], na.rm = TRUE)
      
      if(train_sd > 0) {
        train_data[train_idx, col] <- (train_data[train_idx, col] - train_mean) / train_sd
        
        # Testing data
        test_idx <- test_data$Ticker == tick
        test_data[test_idx, col] <- (test_data[test_idx, col] - train_mean) / train_sd
      }
    }
  }
  
  # Output data summary
  log_message(paste("Training set dimensions:", nrow(train_data), "x", ncol(train_data)))
  log_message(paste("Testing set dimensions:", nrow(test_data), "x", ncol(test_data)))
  log_message(paste("Training set date range:", min(train_data$Date), "to", max(train_data$Date)))
  log_message(paste("Testing set date range:", min(test_data$Date), "to", max(test_data$Date)))
  
  # Save the processed data
  result <- list(
    train = train_data,
    test = test_data
  )
  
  saveRDS(result, "processed_market_data.rds")
  log_message("Data processing completed and saved")
  
  return(result)
}

# Execute preprocessing
processed_data <- preprocess_market_data()
