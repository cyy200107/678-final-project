# 加载必要的包
library(MASS)
library(brms)      # 贝叶斯回归
library(rstanarm)  # 贝叶斯工具
library(dplyr)
library(Matrix)
library(nlme)      # 混合效应模型
library(lme4)      # 混合效应模型扩展
library(zoo)       # 用于填充NA值

# 改进的日志函数
log_message <- function(msg, data = NULL, level = "INFO") {
  timestamp <- format(Sys.time(), "%Y-%m-%d %H:%M:%S")
  prefix <- switch(level,
                   "INFO" = "",
                   "WARNING" = "警告: ",
                   "ERROR" = "错误: ",
                   "DEBUG" = "调试: ")
  
  formatted_msg <- paste0("[", timestamp, "] ", prefix, msg)
  message(formatted_msg)
  
  if(!is.null(data)) {
    if(is.data.frame(data)) {
      print(head(data))
      if(nrow(data) > 6) {
        message("... 仅显示前6行，总行数: ", nrow(data))
      }
    } else {
      print(data)
    }
  }
  
  invisible(formatted_msg)
}

# 改进的数据诊断函数
diagnose_data <- function(data, name = "") {
  log_message(paste("诊断数据:", name))
  
  # 基础统计
  log_message(paste("维度:", nrow(data), "x", ncol(data)))
  log_message("数据结构:")
  str(data)
  log_message("前5行数据:")
  print(head(data))
  
  # NA值分析
  na_stats <- colSums(is.na(data))
  log_message("NA值统计:")
  print(na_stats)
  
  # 数值列统计
  numeric_data <- data[sapply(data, is.numeric)]
  if(ncol(numeric_data) > 0) {
    log_message("数值列统计:")
    stats <- summary(numeric_data)
    print(stats)
  }
  
  # 分组统计
  if("Ticker" %in% colnames(data)) {
    log_message("按Ticker分组的记录数:")
    ticker_counts <- table(data$Ticker)
    print(ticker_counts)
    
    # 检查每个Ticker的时间连续性
    if("Date" %in% colnames(data)) {
      ticker_gaps <- data %>%
        group_by(Ticker) %>%
        arrange(Date) %>%
        summarise(
          gaps = sum(diff(as.numeric(Date)) > 1),
          max_gap = max(diff(as.numeric(Date)))
        )
      log_message("时间序列完整性检查:", ticker_gaps)
    }
  }
}

# 改进的数据预处理函数 - 添加NA值处理
prepare_model_data <- function(data, train_period = NULL) {
  log_message("开始数据预处理和分组...")
  
  # 初始数据诊断
  diagnose_data(data, "原始数据")
  
  # 时间周期处理
  if(!is.null(train_period)) {
    data <- data %>%
      filter(Date >= train_period[1] & Date <= train_period[2])
    log_message("筛选时间周期:", train_period)
  }
  
  # 处理NA值
  data <- data %>%
    group_by(Ticker) %>%
    arrange(Date) %>%
    mutate(across(where(is.numeric),
                  ~na.locf(na.locf(., na.rm = FALSE), fromLast = TRUE))) %>%
    ungroup()
  
  # 区分股票和指数数据
  stocks <- data %>%
    filter(Ticker %in% c("AAPL", "XOM", "NVDA", "TSLA"))
  indices <- data %>%
    filter(Ticker %in% c("SP500", "NASDAQ"))
  
  # 检查分组结果
  diagnose_data(stocks, "股票数据")
  diagnose_data(indices, "指数数据")
  
  # 改进的指数匹配逻辑
  tech_stocks <- c("AAPL", "NVDA", "TSLA")
  
  # 先处理股票数据
  merged_data <- stocks %>%
    mutate(
      Index_Type = ifelse(Ticker %in% tech_stocks, "NASDAQ", "SP500"),
      Stock_Type = ifelse(Ticker %in% tech_stocks, "Tech", "NonTech")
    )
  
  # 重新组织指数数据
  index_data <- indices %>%
    select(Date, Ticker, Close) %>%
    group_by(Date) %>%
    tidyr::pivot_wider(
      names_from = Ticker,
      values_from = Close
    ) %>%
    ungroup()
  
  # 检查指数数据的列名
  log_message("指数数据列名:", names(index_data))
  
  # 合并数据并处理NA值
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
  
  # 标准化处理
  numeric_cols <- c("Close", "MA5", "MA20", "RSI", "Volatility")
  
  # 分组标准化 - 添加错误处理
  merged_data <- merged_data %>%
    group_by(Ticker) %>%
    mutate(across(all_of(numeric_cols), function(x) {
      if(all(is.na(x))) return(x)
      if(sd(x, na.rm = TRUE) == 0) return(x - mean(x, na.rm = TRUE))
      as.vector(scale(x))
    })) %>%
    ungroup()
  
  # 指数数据标准化
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
  
  # 最终数据检查
  diagnose_data(merged_data, "标准化后数据")
  
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

# 改进的批处理函数
batch_processor <- function(data, func, batch_size = 1000, min_batch = 100) {
  log_message(paste("开始批处理, 批次大小:", batch_size))
  
  # 动态批次大小调整
  n <- nrow(data)
  if(n < min_batch) {
    log_message("数据量较小，不进行批处理", level = "WARNING")
    return(func(data))
  }
  
  # 智能批次划分
  optimal_batch_size <- min(batch_size, max(min_batch, ceiling(n/5)))
  batches <- split(1:n, ceiling((1:n)/optimal_batch_size))
  
  # 结果存储
  results <- vector("list", length(batches))
  
  # 诊断第一个批次
  first_batch <- data[batches[[1]],]
  diagnose_data(first_batch, "第一个批次")
  
  # 批次处理
  for(i in seq_along(batches)) {
    log_message(paste("处理批次", i, "/", length(batches)))
    batch_data <- data[batches[[i]],]
    
    results[[i]] <- tryCatch({
      res <- func(batch_data)
      if(i == 1) {
        log_message("第一个批次结果类型:", class(res))
        log_message("第一个批次结果结构:")
        str(res)
      }
      res
    }, error = function(e) {
      log_message(paste("批次处理错误:", e$message), level = "ERROR")
      log_message("问题批次数据概要:")
      diagnose_data(batch_data, paste("错误批次", i))
      NULL
    })
    
    # 内存管理
    if(i %% 3 == 0) gc()
  }
  
  # 结果验证和合并
  valid_results <- Filter(Negate(is.null), results)
  if(length(valid_results) == 0) {
    stop("所有批次处理失败")
  }
  
  combine_results(valid_results)
}

# 改进的结果合并函数 - 添加更多模型类型的处理
combine_results <- function(results) {
  log_message("合并结果...")
  log_message("结果类型:", class(results[[1]]))
  
  if(length(results) == 0) return(NULL)
  
  tryCatch({
    if(is.list(results[[1]]) && all(sapply(results[[1]], inherits, what = "lm"))) {
      # 处理多个线性模型的情况
      log_message("合并多个线性模型结果")
      combined_models <- list()
      for(model_name in names(results[[1]])) {
        combined_models[[model_name]] <- results[[1]][[model_name]]
      }
      return(combined_models)
    }
    else if(inherits(results[[1]], "lm") ||
            inherits(results[[1]], "glm") ||
            inherits(results[[1]], "rlm")) {
      log_message("合并单个回归模型结果")
      return(results[[1]])
    }
    else if(inherits(results[[1]], "lme")) {
      log_message("合并混合效应模型结果")
      return(results[[1]])
    }
    else if(inherits(results[[1]], "brmsfit")) {
      log_message("合并贝叶斯模型结果")
      return(results[[1]])
    }
    else if(is.list(results[[1]]) && "coefficients" %in% names(results[[1]])) {
      log_message("合并系数结果")
      log_message("第一个结果的系数:")
      print(results[[1]]$coefficients)
      
      # 加权平均合并
      n_obs <- sapply(results, function(x) length(residuals(x)))
      weights <- n_obs / sum(n_obs)
      
      # 计算加权平均系数
      all_coeffs <- do.call(rbind, lapply(seq_along(results), function(i) {
        results[[i]]$coefficients * weights[i]
      }))
      combined_coeffs <- colSums(all_coeffs)
      
      # 计算合并后的标准误
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
    
    log_message("使用默认合并策略")
    results[[1]]
    
  }, error = function(e) {
    log_message(paste("合并结果时出错:", e$message), level = "ERROR")
    log_message("结果结构:")
    str(results)
    NULL
  })
}

# 模型预测函数 - 改进多模型预测处理
predict_models <- function(models, newdata) {
  if(is.list(models) && !inherits(models, c("brmsfit", "lme", "lm", "glm", "rlm"))) {
    # 处理多个模型的情况
    predictions <- list()
    for(model_name in names(models)) {
      if(!is.null(models[[model_name]])) {
        tryCatch({
          pred <- predict_single_model(models[[model_name]], newdata)
          if(!is.null(pred)) predictions[[model_name]] <- pred
        }, error = function(e) {
          log_message(paste("预测失败:", model_name, "-", e$message),
                      level = "ERROR")
        })
      }
    }
    
    if(length(predictions) == 0) {
      return(NULL)
    }
    
    # 计算平均预测值
    pred_matrix <- do.call(cbind, predictions)
    return(rowMeans(pred_matrix, na.rm = TRUE))
  } else {
    return(predict_single_model(models, newdata))
  }
}

# 单个模型预测函数
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
      stop("不支持的模型类型")
    }
  }, error = function(e) {
    log_message(paste("预测失败:", e$message), level = "ERROR")
    NULL
  })
}

# 改进的No Pooling模型函数
no_pooling_model <- function(data) {
  log_message("执行No Pooling模型...")
  
  stocks_data <- data$stocks
  diagnose_data(stocks_data, "No Pooling输入数据")
  
  results <- list()
  
  for(stock in unique(stocks_data$Ticker)) {
    log_message(paste("处理股票:", stock))
    stock_data <- stocks_data[stocks_data$Ticker == stock,]
    
    if(nrow(stock_data) == 0) {
      log_message(paste("警告: 没有", stock, "的数据"), level = "WARNING")
      next
    }
    
    if(any(is.na(stock_data$Close)) || any(is.na(stock_data$MA5)) ||
       any(is.na(stock_data$MA20)) || any(is.na(stock_data$RSI)) ||
       any(is.na(stock_data$Volatility))) {
      log_message(paste("警告:", stock, "存在NA值，进行填充"), level = "WARNING")
      stock_data <- stock_data %>%
        arrange(Date) %>%
        mutate(across(c(Close, MA5, MA20, RSI, Volatility),
                      ~na.locf(na.locf(., na.rm = FALSE), fromLast = TRUE)))
    }
    
    # 线性回归
    tryCatch({
      results[[paste0(stock, "_linear")]] <- batch_processor(stock_data, function(d) {
        model <- lm(Close ~ MA5 + MA20 + RSI + Volatility, data = d)
        log_message(paste("线性回归模型摘要 -", stock))
        print(summary(model))
        model
      })
    }, error = function(e) {
      log_message(paste("线性回归失败:", stock, "-", e$message), level = "ERROR")
    })
    
    # 贝叶斯回归
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
          refresh = 0  # 抑制进度输出
        )
        
        log_message(paste("贝叶斯模型摘要 -", stock))
        print(summary(model))
        model
      })
    }, error = function(e) {
      log_message(paste("贝叶斯回归失败:", stock, "-", e$message), level = "ERROR")
    })
    
    # GLM
    tryCatch({
      results[[paste0(stock, "_glm")]] <- batch_processor(stock_data, function(d) {
        model <- glm(Close ~ MA5 + MA20 + RSI + Volatility,
                     family = gaussian(link = "identity"),
                     data = d)
        log_message(paste("GLM模型摘要 -", stock))
        print(summary(model))
        model
      })
    }, error = function(e) {
      log_message(paste("GLM失败:", stock, "-", e$message), level = "ERROR")
    })
    
    gc()
  }
  
  log_message("No Pooling模型完成")
  log_message("结果概览:")
  print(names(results))
  
  return(results)
}

# 改进的Partial Pooling模型函数
partial_pooling_model <- function(data) {
  log_message("执行Partial Pooling模型...")
  
  merged_data <- data$merged
  
  if(any(is.na(merged_data$Close)) || any(is.na(merged_data$Index_Close))) {
    log_message("警告: 数据中存在NA值，进行填充", level = "WARNING")
    merged_data <- merged_data %>%
      group_by(Ticker) %>%
      arrange(Date) %>%
      mutate(across(c(Close, Index_Close, MA5, MA20, RSI, Volatility),
                    ~na.locf(na.locf(., na.rm = FALSE), fromLast = TRUE))) %>%
      ungroup()
  }
  
  diagnose_data(merged_data, "Partial Pooling输入数据")
  
  results <- list()
  
  # 模块1: 分层贝叶斯模型
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
        refresh = 0  # 抑制进度输出
      )
      
      log_message("层次贝叶斯模型摘要:")
      print(summary(model))
      
      model
    })
  }, error = function(e) {
    log_message(paste("层次贝叶斯模型失败:", e$message), level = "ERROR")
  })
  
  # 模块2: 混合效应模型
  tryCatch({
    results$mixed_effects <- batch_processor(merged_data, function(d) {
      model <- lme(Close ~ MA5 + MA20 + RSI + Volatility + Index_Close,
                   random = list(Index_Type = ~ 1, Ticker = ~ 1),
                   data = d,
                   method = "REML",
                   control = lmeControl(opt = "optim",
                                        maxIter = 200,
                                        msMaxIter = 200))
      
      log_message("混合效应模型摘要:")
      print(summary(model))
      
      model
    })
  }, error = function(e) {
    log_message(paste("混合效应模型失败:", e$message), level = "ERROR")
  })
  
  log_message("Partial Pooling模型完成")
  log_message("结果概览:", names(results))
  
  return(results)
}

# 改进的Complete Pooling模型函数
complete_pooling_model <- function(data) {
  log_message("执行Complete Pooling模型...")
  
  merged_data <- data$merged
  
  if(any(is.na(merged_data$Close)) || any(is.na(merged_data$Index_Close))) {
    log_message("警告: 数据中存在NA值，进行填充", level = "WARNING")
    merged_data <- merged_data %>%
      group_by(Ticker) %>%
      arrange(Date) %>%
      mutate(across(c(Close, Index_Close, MA5, MA20, RSI, Volatility),
                    ~na.locf(na.locf(., na.rm = FALSE), fromLast = TRUE))) %>%
      ungroup()
  }
  
  diagnose_data(merged_data, "Complete Pooling输入数据")
  
  results <- list()
  
  # 模块1: 稳健线性回归
  tryCatch({
    results$robust_regression <- batch_processor(merged_data, function(d) {
      model <- rlm(Close ~ MA5 + MA20 + RSI + Volatility + Index_Close,
                   data = d,
                   psi = psi.huber,
                   method = "MM")
      
      log_message("稳健回归模型摘要:")
      print(summary(model))
      model
    })
  }, error = function(e) {
    log_message(paste("稳健回归失败:", e$message), level = "ERROR")
  })
  
  # 模块2: 贝叶斯池化回归
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
        refresh = 0  # 抑制进度输出
      )
      
      log_message("池化贝叶斯模型摘要:")
      print(summary(model))
      model
    })
  }, error = function(e) {
    log_message(paste("池化贝叶斯回归失败:", e$message), level = "ERROR")
  })
  
  log_message("Complete Pooling模型完成")
  log_message("结果概览:", names(results))
  
  return(results)
}

# 改进的评估函数
evaluate_model <- function(models, test_data, actual = NULL) {
  log_message("开始评估模型...")
  log_message("模型类型:", class(models))
  
  if(is.null(models)) {
    log_message("警告: 模型为空", level = "WARNING")
    return(list(RMSE = NA, MAE = NA, R2 = NA, MAPE = NA, DA = NA, IR = NA))
  }
  
  tryCatch({
    # 获取预测值
    predictions <- predict_models(models, test_data)
    if(is.null(predictions)) {
      log_message("警告: 预测失败", level = "WARNING")
      return(list(RMSE = NA, MAE = NA, R2 = NA, MAPE = NA, DA = NA, IR = NA))
    }
    
    actual <- if(is.null(actual)) test_data$Close else actual
    
    # NA值处理
    valid_idx <- !is.na(predictions) & !is.na(actual)
    predictions <- predictions[valid_idx]
    actual <- actual[valid_idx]
    
    if(length(predictions) == 0) {
      log_message("警告: 没有有效预测值", level = "WARNING")
      return(list(RMSE = NA, MAE = NA, R2 = NA, MAPE = NA, DA = NA, IR = NA))
    }
    
    # 计算评估指标
    rmse <- sqrt(mean((predictions - actual)^2))
    mae <- mean(abs(predictions - actual))
    r2 <- 1 - sum((predictions - actual)^2) / sum((actual - mean(actual))^2)
    
    # 处理MAPE的可能的零除
    actual_nonzero <- actual != 0
    mape <- if(any(actual_nonzero)) {
      mean(abs((predictions[actual_nonzero] - actual[actual_nonzero])/
                 actual[actual_nonzero])) * 100
    } else {
      NA
    }
    
    # 方向准确率
    direction_actual <- c(NA, diff(actual))
    direction_pred <- c(NA, diff(predictions))
    da <- mean(sign(direction_actual) == sign(direction_pred), na.rm = TRUE)
    
    # 信息比率 - 处理零标准差
    returns_pred <- (predictions - actual) / abs(actual)
    returns_sd <- sd(returns_pred, na.rm = TRUE)
    ir <- if(!is.na(returns_sd) && returns_sd > 0) {
      mean(returns_pred, na.rm = TRUE) / returns_sd
    } else {
      NA
    }
    
    # 输出结果
    results <- list(
      RMSE = rmse,
      MAE = mae,
      R2 = r2,
      MAPE = mape,
      DA = da,
      IR = ir,
      n_predictions = length(predictions)
    )
    
    log_message("评估指标:")
    print(results)
    
    results
    
  }, error = function(e) {
    log_message(paste("评估错误:", e$message), level = "ERROR")
    list(RMSE = NA, MAE = NA, R2 = NA, MAPE = NA, DA = NA, IR = NA)
  })
}

# 改进的交叉验证函数
cross_validate <- function(data, k = 5, seed = 42) {
  log_message(paste("开始", k, "折交叉验证"))
  
  set.seed(seed)
  
  # 确保数据按日期排序
  data <- data %>% arrange(Date)
  
  # 获取唯一的日期
  unique_dates <- unique(data$Date)
  n_dates <- length(unique_dates)
  fold_size <- floor(n_dates / k)
  
  # 存储所有结果
  results <- list()
  
  for(i in 1:k) {
    log_message(paste("处理第", i, "折"))
    
    # 计算时间窗口
    if(i < k) {
      test_dates <- unique_dates[((i-1)*fold_size + 1):(i*fold_size)]
    } else {
      test_dates <- unique_dates[((i-1)*fold_size + 1):n_dates]
    }
    
    # 分割数据
    test_data <- data %>% filter(Date %in% test_dates)
    train_data <- data %>% filter(!(Date %in% test_dates))
    
    # 检查分割结果
    log_message(paste("训练集大小:", nrow(train_data), "行,",
                      length(unique(train_data$Date)), "个交易日"))
    log_message(paste("测试集大小:", nrow(test_data), "行,",
                      length(unique(test_data$Date)), "个交易日"))
    
    # 数据预处理
    train_processed <- prepare_model_data(train_data)
    test_processed <- prepare_model_data(test_data)
    
    # 模型训练和评估
    fold_results <- tryCatch({
      # No Pooling模型
      no_pooling <- no_pooling_model(train_processed)
      
      # Partial Pooling模型
      partial_pooling <- partial_pooling_model(train_processed)
      
      # Complete Pooling模型
      complete_pooling <- complete_pooling_model(train_processed)
      
      # 评估结果
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
      log_message(paste("折叠处理错误:", e$message), level = "ERROR")
      NULL
    })
    
    # 保存当前折的结果
    results[[i]] <- fold_results
    
    # 输出当前折的评估结果
    if(!is.null(fold_results)) {
      log_message(paste("第", i, "折评估结果:"))
      print(fold_results$evaluation)
    }
    
    # 内存管理
    gc()
  }
  
  # 验证结果
  valid_results <- Filter(Negate(is.null), results)
  if(length(valid_results) == 0) {
    stop("所有交叉验证折叠都失败了")
  }
  
  # 汇总结果
  summarize_cv_results(valid_results, k)
}

# 改进的评估单折结果函数
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
        log_message(paste("评估失败:", model_type, "-", e$message), level = "ERROR")
        evaluations[[model_type]] <- list(
          RMSE = NA, MAE = NA, R2 = NA, MAPE = NA, DA = NA, IR = NA
        )
      })
    }
  }
  
  return(evaluations)
}

# 改进的交叉验证结果汇总函数
summarize_cv_results <- function(results, k) {
  log_message(paste("汇总", k, "折交叉验证结果"))
  
  metrics <- c("RMSE", "MAE", "R2", "MAPE", "DA", "IR")
  summary <- list()
  
  # 获取所有模型类型
  model_types <- unique(unlist(lapply(results, function(x) {
    if(!is.null(x$evaluation)) names(x$evaluation)
  })))
  
  for(model_type in model_types) {
    model_metrics <- matrix(NA, nrow = k, ncol = length(metrics))
    colnames(model_metrics) <- metrics
    
    # 收集所有折的结果
    for(i in seq_along(results)) {
      if(!is.null(results[[i]]$evaluation[[model_type]])) {
        model_metrics[i,] <- sapply(metrics, function(m) {
          results[[i]]$evaluation[[model_type]][[m]]
        })
      }
    }
    
    # 计算均值和标准差
    summary[[model_type]] <- list(
      means = colMeans(model_metrics, na.rm = TRUE),
      sds = apply(model_metrics, 2, sd, na.rm = TRUE),
      all_folds = model_metrics
    )
  }
  
  log_message("交叉验证结果汇总:")
  print(summary)
  
  return(summary)
}

# 改进的报告生成函数
generate_report <- function(results, models = NULL) {
  log_message("生成分析报告...")
  
  tryCatch({
    # 输出文件路径
    output_file <- "model_analysis_report.txt"
    
    # 写入报告
    sink(output_file)
    
    cat("=== 金融市场建模分析报告 ===\n")
    cat("生成时间:", format(Sys.time(), "%Y-%m-%d %H:%M:%S"), "\n\n")
    
    # 模型性能总结
    cat("1. 模型性能总结\n")
    cat("----------------\n")
    for(model_name in names(results)) {
      cat("\n模型:", model_name, "\n")
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
    
    # 模型比较
    cat("\n2. 模型比较\n")
    cat("------------\n")
    comparison <- do.call(rbind, lapply(results, function(x) {
      if(!is.null(x$means)) x$means else rep(NA, 6)
    }))
    rownames(comparison) <- names(results)
    print(round(comparison, 4))
    
    # 如果提供了模型对象，添加模型诊断
    if(!is.null(models)) {
      cat("\n3. 模型诊断\n")
      cat("------------\n")
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
                cat("  无法生成模型摘要:", e$message, "\n")
              })
            }
          }
        }
      }
    }
    
    sink()
    
    log_message(paste("报告已生成:", output_file))
    
  }, error = function(e) {
    log_message(paste("报告生成失败:", e$message), level = "ERROR")
  })
}

# 数据对齐检查函数
check_data_alignment <- function(processed_data) {
  log_message("检查数据时间对齐...")
  
  # 训练集检查
  train_alignment <- check_single_dataset(processed_data$train, "训练集")
  
  # 测试集检查
  test_alignment <- check_single_dataset(processed_data$test, "测试集")
  
  return(list(
    train = train_alignment,
    test = test_alignment
  ))
}

# 辅助函数：检查单个数据集的对齐情况
check_single_dataset <- function(data, dataset_name) {
  if(is.null(data)) {
    stop(paste(dataset_name, "数据为空"))
  }
  
  # 检查每个Ticker的日期范围
  date_ranges <- data %>%
    group_by(Ticker) %>%
    summarise(
      min_date = min(Date),
      max_date = max(Date),
      n_dates = n_distinct(Date)
    )
  
  log_message(paste(dataset_name, "各Ticker的日期范围:"))
  print(date_ranges)
  
  # 检查是否所有Ticker都有相同的日期
  all_dates <- data %>%
    group_by(Date) %>%
    summarise(
      n_tickers = n_distinct(Ticker),
      tickers = paste(sort(unique(Ticker)), collapse = ", ")
    ) %>%
    filter(n_tickers != n_distinct(data$Ticker))
  
  if(nrow(all_dates) > 0) {
    log_message(paste("警告:", dataset_name, "发现不完整的日期:"), level = "WARNING")
    print(all_dates)
  }
  
  # 检查数据连续性
  ticker_gaps <- data %>%
    group_by(Ticker) %>%
    arrange(Date) %>%
    summarise(
      gaps = sum(diff(as.numeric(Date)) > 1),
      max_gap = max(diff(as.numeric(Date)))
    )
  
  log_message(paste(dataset_name, "时间连续性检查:"))
  print(ticker_gaps)
  
  return(list(
    date_ranges = date_ranges,
    incomplete_dates = all_dates,
    gaps = ticker_gaps
  ))
}

# 辅助函数：检查数据质量
check_data_quality <- function(data, dataset_name) {
  log_message(paste("检查", dataset_name, "数据质量..."))
  
  # 检查必要列
  required_cols <- c("Date", "Ticker", "Close", "High", "Low", "Open",
                     "Volume", "Returns", "RSI", "MA5", "MA20", "Volatility")
  missing_cols <- setdiff(required_cols, colnames(data))
  
  if(length(missing_cols) > 0) {
    log_message(paste("警告:", dataset_name, "缺少列:",
                      paste(missing_cols, collapse = ", ")), level = "WARNING")
  }
  
  # 检查NA值
  na_counts <- colSums(is.na(data))
  if(any(na_counts > 0)) {
    log_message(paste(dataset_name, "NA值统计:"))
    print(na_counts[na_counts > 0])
  }
  
  # 检查异常值
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
    log_message(paste(dataset_name, "异常值统计:"))
    print(outliers[outliers > 0])
  }
  
  return(list(
    missing_columns = missing_cols,
    na_counts = na_counts,
    outliers = outliers
  ))
}

# 主函数
main <- function() {
  log_message("开始主程序执行...")
  
  tryCatch({
    # 读取预处理过的数据
    log_message("读取数据...")
    processed_data <- readRDS("processed_market_data.rds")
    
    if(is.null(processed_data)) {
      stop("processed_market_data.rds 数据为空")
    }
    
    # 检查数据完整性
    log_message("检查数据完整性...")
    alignment_check <- check_data_alignment(processed_data)
    
    # 如果发现数据不完整，进行修复
    if(nrow(alignment_check$train$incomplete_dates) > 0 ||
       nrow(alignment_check$test$incomplete_dates) > 0) {
      log_message("修复数据时间对齐问题...")
      
      # 训练集处理
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
      
      # 测试集处理
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
      
      # 填充NA值
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
      
      log_message("数据修复完成")
      log_message(paste("训练集: 原始行数:", nrow(processed_data$train),
                        "修复后行数:", nrow(fixed_train)))
      log_message(paste("测试集: 原始行数:", nrow(processed_data$test),
                        "修复后行数:", nrow(fixed_test)))
      
      processed_data$train <- fixed_train
      processed_data$test <- fixed_test
    }
    
    # 使用训练集数据
    data <- processed_data$train
    
    if(is.null(data)) {
      stop("训练集数据为空")
    }
    
    # 检查数据结构
    required_cols <- c("Date", "Ticker", "Close", "High", "Low", "Open",
                       "Volume", "Returns", "RSI", "MA5", "MA20", "Volatility")
    missing_cols <- setdiff(required_cols, colnames(data))
    if(length(missing_cols) > 0) {
      log_message(paste("当前列名:", paste(colnames(data), collapse = ", ")))
      stop(paste("数据缺少必要列:", paste(missing_cols, collapse = ", ")))
    }
    
    # 检查股票代码
    required_tickers <- c("AAPL", "XOM", "NVDA", "TSLA", "SP500", "NASDAQ")
    missing_tickers <- setdiff(required_tickers, unique(data$Ticker))
    if(length(missing_tickers) > 0) {
      stop(paste("数据缺少必要的股票代码:", paste(missing_tickers, collapse = ", ")))
    }
    
    log_message("数据加载成功，开始建模...")
    log_message(paste("数据维度:", nrow(data), "x", ncol(data)))
    log_message(paste("日期范围:", min(data$Date), "至", max(data$Date)))
    log_message("股票数量统计:")
    print(table(data$Ticker))
    
    # 执行交叉验证
    log_message("开始交叉验证...")
    cv_results <- cross_validate(data, k = 5)
    
    # 在完整数据集上训练最终模型
    log_message("开始在完整数据集上训练模型...")
    model_data <- prepare_model_data(data)
    
    # 训练模型
    log_message("开始各类型模型训练...")
    models <- list()
    
    # No Pooling
    log_message("训练No Pooling模型...")
    models$no_pooling <- tryCatch({
      no_pooling_model(model_data)
    }, error = function(e) {
      log_message(paste("No Pooling模型失败:", e$message), level = "ERROR")
      NULL
    })
    
    # Partial Pooling
    log_message("训练Partial Pooling模型...")
    models$partial_pooling <- tryCatch({
      partial_pooling_model(model_data)
    }, error = function(e) {
      log_message(paste("Partial Pooling模型失败:", e$message), level = "ERROR")
      NULL
    })
    
    # Complete Pooling
    log_message("训练Complete Pooling模型...")
    models$complete_pooling <- tryCatch({
      complete_pooling_model(model_data)
    }, error = function(e) {
      log_message(paste("Complete Pooling模型失败:", e$message), level = "ERROR")
      NULL
    })
    
    # 评估测试集表现
    log_message("评估测试集表现...")
    test_data <- processed_data$test
    if(!is.null(test_data)) {
      test_results <- list()
      test_data_processed <- prepare_model_data(test_data)
      
      for(model_type in names(models)) {
        if(!is.null(models[[model_type]])) {
          test_results[[model_type]] <- tryCatch({
            evaluate_model(models[[model_type]], test_data_processed$merged)
          }, error = function(e) {
            log_message(paste("测试集评估失败:", model_type, "-", e$message),
                        level = "ERROR")
            NULL
          })
        }
      }
      
      log_message("测试集评估结果:")
      print(test_results)
    } else {
      log_message("警告: 没有测试集数据", level = "WARNING")
    }
    
    # 生成最终报告
    log_message("生成分析报告...")
    generate_report(cv_results, models)
    
    # 保存结果
    log_message("保存模型和结果...")
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
    
    log_message("程序执行完成!")
    return(final_results)
    
  }, error = function(e) {
    log_message(paste("程序执行失败:", e$message), level = "ERROR")
    return(NULL)
  })
}

# 启动程序
if(sys.nframe() == 0) {
  # 设置并行计算核心数
  options(mc.cores = parallel::detectCores()-1)
  
  # 设置随机数种子
  set.seed(42)
  
  # 启动主程序
  results <- main()
  
  # 检查结果
  if(!is.null(results)) {
    cat("\n程序执行成功!\n")
    cat("模型类型:", names(results$models), "\n")
    cat("交叉验证次数:", length(results$cv_results), "\n")
  } else {
    cat("\n程序执行失败!\n")
  }
}