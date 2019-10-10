#!/bin/bash
nice Rscript --verbose pairs_trading_sp500_etf100_rolling.R >& ../log/pairs_trading_sp500_etf100_rolling.log;
nice Rscript --verbose pairs_trading_same_ind_rolling.R >& ../log/pairs_trading_same_ind_rolling.log;
nice Rscript --verbose pairs_trading_sp500_sp500_rolling.R >& ../log/pairs_trading_sp500_sp500_rolling.log;
nice Rscript --verbose pairs_trading_sp400_sp400_rolling.R >& ../log/pairs_trading_sp400_sp400_rolling.log;
nice Rscript --verbose pairs_trading_sp600_sp600_rolling.R >& ../log/pairs_trading_sp600_sp600_rolling.log; 
nice Rscript --verbose pairs_trading_same_sector_rolling.R >& ../log/pairs_trading_same_sector_rolling.log;
nice Rscript --verbose pairs_trading_all_rolling.R >& ../log/pairs_trading_all_rolling.log;
