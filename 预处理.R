# 加载必要的包
library(MASS)      
library(brms)      
library(rstanarm)  
library(dplyr)     

# 文件路径设置
file_paths <- list(
  AAPL = "~/Desktop/AAPL_stock_data.csv",
  XOM = "~/Desktop/XOM_5_years_data.csv",
  NVDA = "~/Desktop/NVDA_5_years_data.csv",
  TSLA = "~/Desktop/TSLA_5_years_data.csv",
  SP500 = "~/Desktop/SP500_data.csv",
  NASDAQ = "~/Desktop/NASDAQ_Composite_data.csv"
)

# 日志函数
log_message <- function(msg) {
  timestamp <- format(Sys.time(), "%Y-%m-%d %H:%M:%S")
  message(paste0("[", timestamp, "] ", msg))
  flush.console()
}

# 移动平均计算函数 - 使用基本运算
calculate_ma <- function(x, window) {
  n <- length(x)
  result <- vector("numeric", n)
  result[1:(window-1)] <- NA
  for(i in window:n) {
    result[i] <- mean(x[(i-window+1):i], na.rm = TRUE)
  }
  return(result)
}

# 波动率计算函数 - 使用基本运算
calculate_volatility <- function(returns, window = 20) {
  n <- length(returns)
  result <- vector("numeric", n)
  result[1:(window-1)] <- NA
  for(i in window:n) {
    result[i] <- sd(returns[(i-window+1):i], na.rm = TRUE)
  }
  return(result)
}

# 数据加载函数
load_stock_data <- function(file_path, ticker) {
  log_message(paste("读取", ticker, "数据"))
  
  data <- tryCatch({
    df <- read.csv(file_path)
    df$Ticker <- ticker
    df$Date <- as.Date(gsub(" .*$", "", df$Date))
    df[, c("Date", "Ticker", "Close", "High", "Low", "Open", "Volume")]
  }, error = function(e) {
    log_message(paste("错误: 无法读取", ticker, "数据 -", e$message))
    return(NULL)
  })
  
  if(!is.null(data)) {
    log_message(paste(ticker, "数据加载完成，行数:", nrow(data)))
  }
  return(data)
}

# RSI计算函数 - 使用基本运算
calculate_rsi <- function(prices, window = 14) {
  diffs <- diff(prices)
  gains <- pmax(0, diffs)
  losses <- abs(pmin(0, diffs))
  
  result <- rep(NA, length(prices))
  
  for(i in (window+1):length(prices)) {
    avg_gain <- mean(gains[(i-window):(i-1)], na.rm = TRUE)
    avg_loss <- mean(losses[(i-window):(i-1)], na.rm = TRUE)
    rs <- avg_gain / (avg_loss + 1e-10)  # 避免除以0
    result[i] <- 100 * (1 - 1/(1 + rs))
  }
  
  return(result)
}

# 特征计算函数
calculate_features <- function(data) {
  # 按日期排序
  data <- data[order(data$Date), ]
  
  # 初始化结果
  result <- data
  result$Returns <- NA
  result$RSI <- NA
  result$MA5 <- NA
  result$MA20 <- NA
  result$Volatility <- NA
  
  # 按Ticker计算所有指标
  for(tick in unique(data$Ticker)) {
    idx <- which(data$Ticker == tick)
    prices <- data$Close[idx]
    
    # 计算收益率
    result$Returns[idx] <- c(NA, diff(prices)/prices[-length(prices)])
    
    # 计算RSI
    result$RSI[idx] <- calculate_rsi(prices)
    
    # 计算移动平均
    result$MA5[idx] <- calculate_ma(prices, 5)
    result$MA20[idx] <- calculate_ma(prices, 20)
    
    # 计算波动率
    result$Volatility[idx] <- calculate_volatility(result$Returns[idx])
  }
  
  return(result)
}

# 主预处理函数
preprocess_market_data <- function() {
  log_message("开始数据预处理...")
  
  # 加载所有数据
  all_data <- list()
  for(ticker in names(file_paths)) {
    data <- load_stock_data(file_paths[[ticker]], ticker)
    if(!is.null(data)) {
      all_data[[ticker]] <- data
    }
  }
  
  # 合并数据
  combined_data <- do.call(rbind, all_data)
  log_message(paste("合并数据完成，总行数:", nrow(combined_data)))
  
  # 计算特征
  processed_data <- calculate_features(combined_data)
  log_message("特征计算完成")
  
  # 移除含NA的行
  processed_data <- na.omit(processed_data)
  log_message(paste("清理后数据行数:", nrow(processed_data)))
  
  # 区分股票和指数数据
  stocks_data <- processed_data[processed_data$Ticker %in% c("AAPL", "XOM", "NVDA", "TSLA"), ]
  indices_data <- processed_data[processed_data$Ticker %in% c("SP500", "NASDAQ"), ]
  
  # 数据分割
  total_dates <- sort(unique(processed_data$Date))
  split_idx <- floor(length(total_dates) * 0.8)
  split_date <- total_dates[split_idx]
  
  # 准备训练集和测试集
  train_data <- processed_data[processed_data$Date <= split_date, ]
  test_data <- processed_data[processed_data$Date > split_date, ]
  
  # 标准化
  numeric_cols <- c("Close", "Returns", "RSI", "MA5", "MA20", "Volatility")
  
  # 使用基本R运算进行标准化
  for(col in numeric_cols) {
    for(tick in unique(processed_data$Ticker)) {
      # 训练集
      train_idx <- train_data$Ticker == tick
      train_mean <- mean(train_data[train_idx, col], na.rm = TRUE)
      train_sd <- sd(train_data[train_idx, col], na.rm = TRUE)
      
      if(train_sd > 0) {
        train_data[train_idx, col] <- (train_data[train_idx, col] - train_mean) / train_sd
        
        # 测试集
        test_idx <- test_data$Ticker == tick
        test_data[test_idx, col] <- (test_data[test_idx, col] - train_mean) / train_sd
      }
    }
  }
  
  # 输出数据概要
  log_message(paste("训练集维度:", nrow(train_data), "x", ncol(train_data)))
  log_message(paste("测试集维度:", nrow(test_data), "x", ncol(test_data)))
  log_message(paste("训练集日期范围:", min(train_data$Date), "至", max(train_data$Date)))
  log_message(paste("测试集日期范围:", min(test_data$Date), "至", max(test_data$Date)))
  
  # 保存结果为单一数据框格式
  result <- list(
    train = train_data,
    test = test_data
  )
  
  saveRDS(result, "processed_market_data.rds")
  log_message("数据处理完成并保存")
  
  return(result)
}

# 执行预处理
processed_data <- preprocess_market_data()