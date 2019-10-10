source(Sys.getenv('R_USER_DIR_CFG_FILE'));
source(cfg_file_pairs_trade);

#*****************************************************************
#
# The trading parameters hedgeRatio and trade level is calculated
# on a rolling windows of 6 months, by monthly on a time period 
# of 1.5 year.
#
# Trading using hedgeRation and trade lvl calculated on every
# month.
#
#*****************************************************************

#*****************************************************************
# TODO: 1) non-zero mean crossing
#       2) use bootstrap for estimating num of zero-crossing
#       3)  Kalman lter are one way to calculate time-varying hedge ratios
#       4) use  Total Least Squares  ? betterHedgeRatios.pdf
#******************************************************************

library(quantmod)
library(tseries)
library(PerformanceAnalytics)
library(sit)
source(paste0(cfg_path_global_lib,'/libtrade.R'));
require(doMC);

#*****************************************************************
# configuration
#******************************************************************    

cfg_prescreen_data_file = paste0(cfg_pairs_trade_Rdata_path,'prescreen_sp600_sp600_rolling.rda');
cfg_matched_pairs_data_file = paste0(cfg_pairs_trade_Rdata_path,'matched_pairs_sp600_sp600_rolling.rda');

#*****************************************************************
# Load historical data
#******************************************************************    

data <- new.env()
load_sp600_stockData(data);

for(i in ls(data)) 
{
  sData <- data[[i]]
  oldColNames <- names(sData)
  colnames(sData) <- c("S.Open","S.High","S.Low","S.Close","S.Volume","S.Adjusted")
  sData = adjustOHLC(sData, use.Adjusted=T) 
  colnames(sData) <- oldColNames
  data[[i]] <- sData
}

# important, else the bt.prep will off 1 day.
Sys.setenv(TZ='UTC');
bt.prep(data, align='keep.all', dates='2012::');
# convert the time series back to Date 
convert_index_to_date(data);

#*****************************************************************
# Code Strategies
#******************************************************************

prices <- data$prices;
num_stock <- ncol(prices);

# remove unused data
data <- new.env();
gc();

matched_pairs <- NULL;

registerDoMC(cfg_DoMC_num_processor);
start_time <- Sys.time();

if(file.exists(cfg_prescreen_data_file)){
  load(cfg_prescreen_data_file);
  cat('prescreen loaded from data file ...\n');
} else {
  
  matched_pairs_prescreen <- foreach(i=1:(num_stock-1), .combine = "rbind",
                                     .errorhandling = 'pass') %dopar%
  {
    matched_pairs_tmp <- NULL;
    message(paste(Sys.time(),' | ','Calculating for pre-screen, i = ',i,
                  ' @ process : ',Sys.getpid(), sep=''));
    
    for(j in (i+1):num_stock)
    {
      matched_algo_ret <- NULL;
      try(matched_algo_ret <- matching_algo_prescreen(i, j, prices, prices, cfg_p_value_lmt));
      if(!is.null(matched_algo_ret))
      {
        matched_pairs_tmp <- rbind(matched_pairs_tmp, matched_algo_ret);
      }
    }
    #  print(matched_pairs_tmp);
    matched_pairs_tmp;
  }
  
  save(matched_pairs_prescreen, file=cfg_prescreen_data_file); 
}


m <- matched_pairs_prescreen;

# filter out the pairs with coe > 0.98. Note, a very close to 1 coe caused the
# arima.sim execute extreme long time and huge memory.
m <- m[m$ar1_coe <= 0.98,];
rank_size <- min(nrow(m), cfg_num_prescreen_pairs);

num_crossing_rank <- rank(m$num_crossing, na.last=T);
coe_rank <- rank(-m$ar1_coe, na.last=T);
total_rank <- num_crossing_rank + (2 * coe_rank);
# top 1000
m_top_rank <- m[(order(-total_rank))[1:rank_size],];

#*****************************************************************
# Rolling test
#*****************************************************************

matched_pairs <- foreach(i=1:nrow(m_top_rank),
                         .errorhandling = 'pass') %dopar%
{
  matched_algo_ret <- NULL;
  process_start_time <- Sys.time();
  
  try(matched_algo_ret <- matching_algo.rolling(m_top_rank[i, 'price1'], 
                                                m_top_rank[i, 'price2'], 
                                                prices, prices,
                                                cfg_rolling_width));
  
  message(paste(Sys.time(),' | ','Calculated for Rolling sample, i = ',i,
                ' @ process : ',Sys.getpid(),
                ' CPU time: ',  round(Sys.time() - process_start_time, digits=1),
                sep=''));
  
  matched_algo_ret;
}
# remove NULL element
matched_pairs_list <- matched_pairs[!sapply(matched_pairs, is.null)];

matched_pairs_df <- matched_pairs_extract_df(matched_pairs_list);

cat('The process took: '); Sys.time() - start_time;
registerDoMC();

save(prices, 
     matched_pairs_list,
     matched_pairs_df,
     cfg_num_prescreen_pairs, cfg_p_value_lmt, cfg_rolling_width,
     file=cfg_matched_pairs_data_file);



