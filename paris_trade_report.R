source(Sys.getenv('R_USER_DIR_CFG_FILE'));
source(cfg_file_pairs_trading_rpt);
source(paste0(cfg_path_pairs_trading_lib,'/libtrade.R'));

#*****************************************************************
#
# pairs trade report
#
#*****************************************************************

#*****************************************************************
# TODO:  1) stop lose in trade ?
#        2) potential pairs recalculate the trade level with more simulation ?
#         ) setup portfolio based on a set of rules, then compare the portfolios
#******************************************************************

library(quantmod);
library(PerformanceAnalytics);
library(plyr);

# use local data for getSymbol()
if(cfg_use_local_symbol_lookup)
  set_local_symbol_lookup();

#same_ind_env <- new.env();


load(file=paste0(cfg_path_pt_rpt_data,'matched_pairs_same_ind_rolling.rda'));
#load(file=paste0(cfg_path_pt_rpt_data,'matched_pairs_all_rolling.rda'));
#load(file=paste0(cfg_path_pt_rpt_data,'matched_pairs_sp500_etf100_rolling.rda'));


# use a set of rules to filter potential pairs.


# 2) number of trade must >= 8
#  ) number of crossing >= 20
#  ) the sharpe ratio must be >= 2.5
#  ) the annulized return must be >= 0.3
# 1) the pair must have > 1 yr of historical data
#  ) the max drawndown must be < 25% of annualized return.
# 3) the hedge ratio must be stable and varies within 25%
# 4) the trade level must be stable and varies within 25%
#  ) there is a wide spread > x% of the opt trade level at y% of the time

# not having an unclose trade will large trade duration (runaway pairs)
# those successfully completed a few trade.

# categorize the pot. pairs
#  1) the last data is not in an open trade.
#  2) the last data is not divergence, the spread is not 25% over opt trade level 
#  3) the last data is closing the trade. current spread < spread at open trade
#   ) does the pairs have own characteristic. How diverse from the S&P500


mp <- matched_pairs_df;
idx_df <- data.frame(idx=index(mp));
pot <- cbind(idx_df, mp);

# number of trade must >= 8
pot <- pot[which(pot$sample_num_trade >= 8),];

# number of crossing >= 20
pot <- pot[which(pot$num_crossing >= 20),];

# the sharpe ratio must be >= 2.5
pot <- pot[which(pot$sample_sharpe >= 2.5),];

# the annulized return must be >= 0.3
pot <-pot[which(pot$sample_ret >= 0.1),];


# the pair must have > 1 yr of historical data

check_stock_data_len <- function(row, min_trading_day)
{
  price_idx1 <- row$price1;
  price_idx2 <- row$price2;
  
  price.pair <- merge(na.omit(prices[,price_idx1]), na.omit(prices[,price_idx2]));
  if(NROW(price.pair) >= min_trading_day){
    return(TRUE) } else { return(FALSE) }
}

idx <- which(adply(pot, 1, function(x) check_stock_data_len(x, 252))$V1);
pot <- pot[idx,];


#  ) the max drawndown must be < 25% of annualized return.
# 3) the hedge ratio must be stable and varies within 25%
# 4) the trade level must be stable and varies within 25%
check_rule <- function(x)
{
  pairs_stat <- matched_pairs_list[[x$idx]];
#  daily_ret <- pairs_stat$daily_ret;
#  sig <- pairs_stat$sig;
#  spread <- pairs_stat$spread;
#  opt_trade_lvl <- pairs_stat$opt_trade_lvl;
  rolling_stat <- pairs_stat$rolling_stat;
  
  maxDD <- maxDrawdown(pairs_stat$daily_ret);
  if( (maxDD/x$sample_ret) > 0.25 ){ return(1) }
  
  hr_mean <- mean(rolling_stat$hedge_ratio);
  hr_max <- max(rolling_stat$hedge_ratio);
  hr_min <- min(rolling_stat$hedge_ratio);
  
  if( (hr_max/hr_mean - 1) > 0.15 ||
        (1 - hr_min/hr_mean) > 0.15 ){ return(2) }
  
  otl_mean <- mean(rolling_stat$opt_trade_lvl, na.rm=T);
  otl_max <- max(rolling_stat$opt_trade_lvl, na.rm=T);
  otl_min <- min(rolling_stat$opt_trade_lvl, na.rm=T);
  
  #print(rolling_stat$opt_trade_lvl)

  if( (otl_max/otl_mean - 1) > 0.5 ||
        (1 - otl_min/otl_mean) > 0.5 ){ return(3) }
  
  return(4);
}

idx <- which(adply(pot, 1, function(x) check_rule(x))$V1 == 4);
pot <- pot[idx,];





# calculate latest performance of the pairs
##### extend the series with real time stock data, code from matching_algo.rolling
cal_latest_perf <- function(x)
  {

  pairs_stat <- matched_pairs_list[[x$idx]];
  rolling_stat <- pairs_stat$rolling_stat;
    
  sig <- pairs_stat$sig;
  spread <- pairs_stat$spread;
  opt_trade_lvl <- pairs_stat$opt_trade_lvl;
  
  hmaxDD <- maxDrawdown(pairs_stat$daily_ret)*100;
  
# get stock price of the pairs
# TODO: use cache or local stock data
print(pairs_stat$stock1_sym);
print(pairs_stat$stock2_sym);

stock1 <- NULL; stock2 <- NULL;
try(stock1 <- getSymbols(pairs_stat$stock1_sym, from='2012-01-01', auto.assign=F));
try(stock2 <- getSymbols(pairs_stat$stock2_sym, from='2012-01-01', auto.assign=F));

if(is.null(stock1) || NROW(stock1) == 0 ||
     is.null(stock2) || NROW(stock2) == 0)
  return(NULL);

# if local symbol are used.
stock1 <- stock1['2012-01-01/',];
stock2 <- stock2['2012-01-01/',];

oldColNames <- names(stock1)
colnames(stock1) <- c("S.Open","S.High","S.Low","S.Close","S.Volume","S.Adjusted")
sData = adjustOHLC(stock1, use.Adjusted=T) 
colnames(stock1) <- oldColNames
stock1 = Cl(stock1);

oldColNames <- names(stock2)
colnames(stock2) <- c("S.Open","S.High","S.Low","S.Close","S.Volume","S.Adjusted")
sData = adjustOHLC(stock2, use.Adjusted=T) 
colnames(stock2) <- oldColNames
stock1 = Cl(stock2);


combined <- merge(stock1, stock2);

# get last rolling window
last_roll <- last(pairs_stat$rolling_stat);

row = which((index(combined) > index(last_roll)));

stock_1 <- combined[row,1];
stock_2 <- combined[row,2];

# the hedgeRatio, trade lvl from previous rolling window
hedge_ratio_prev <- as.numeric(last_roll$hedge_ratio);
opt_trade_lvl_prev <- as.numeric(last_roll$opt_trade_lvl);

#print(index(last_roll));
#print(hedge_ratio_prev);
#print(opt_trade_lvl_prev);
#print(last(pairs_stat$spread));

spread_rolling <- stock_1 - hedge_ratio_prev * stock_2
spread_rolling <- spread_rolling[!is.na(spread_rolling)];

#print(spread_rolling); 
# build the same time series as spread_rolling
opt_trade_lvl_rolling <- spread_rolling;
opt_trade_lvl_rolling[] <- opt_trade_lvl_prev;

spread <- rbind(spread, spread_rolling);
opt_trade_lvl <- rbind(opt_trade_lvl, opt_trade_lvl_rolling);

sig_rule1 <- gen_sig_trade_rule1(spread, opt_trade_lvl, -opt_trade_lvl);
row_sig <- which((index(sig_rule1) > index(last(sig))));
sig_rolling <- sig_rule1[row_sig,];
sig <- rbind(sig, sig_rolling);

##### extend the series with real time stock data
if(NROW(which(sig_rolling[,1] != 0)) > 10 ||
   NROW(row > 10 )){
perf_stat <- pairs_perf_stat(combined[row,], sig_rolling);

perf_df <- data.frame(total_ret = (as.numeric(last(perf_stat$cum_ret)) - 100), 
           ret = perf_stat$ret, 
           sharpe = perf_stat$sharpe,
           hmaxDD=hmaxDD );
} else {
  perf_df <- data.frame(total_ret = 0, 
                        ret = 0, 
                        sharpe = 0,
                        hmaxDD = hmaxDD);
}
# print(as.numeric(last(perf_stat$cum_ret)));
# print(perf_df$total_ret);
# print(perf_df$ret);
# print(perf_df$sharpe);

return(perf_df);
}

perf <- ddply(pot, 1, function(x) cal_latest_perf(x));
pot <- merge(pot, perf, all=T)

p <- pot[,c(4,5,6,12,13,14,15,18)]
p[,c(5,6,7,8)] <- round(p[,c(5,6,7,8)], digits=2)
p[order(p$stock1_sym),]
p[order(-p$total_ret),]
p[order(-p$sample_num_trade),]

NROW(which(p$total_ret > 0))
NROW(which(p$total_ret <= 0))
