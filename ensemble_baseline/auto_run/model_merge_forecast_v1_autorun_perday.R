library(dplyr)
library(data.table)
library(xlsx)
options(warn=-1)

# parameter
require(lubridate)
last_month = Sys.Date()
month(last_month) <- month(last_month) + 1

last_day <- function(date) {
  ceiling_date(date, "month") - days(1)
}

first_day <- function(date) {
  ceiling_date(date, "month") - months(1)
}

set_forecast_day_orin = seq(first_day(last_month), last_day(last_month), by = "day")
set_forecast_day = as.numeric(gsub("-","",set_forecast_day_orin))
set_forecast_day_len = length(set_forecast_day)


sale_df = fread('./input_output_file/sale_data_top100_day.csv',encoding = 'UTF-8')
sale_df$date = as.numeric(gsub("-","",sale_df$date))
sale_df$ALCSCM = paste0('0',sale_df$ALCSCM)
sale_df$uniq_ID = paste0(sale_df$good_gid,"_",sale_df$ALCSCM)

# sale_df_set = sale_df[,c('good_gid','date','uniq_ID','sum_sale_qty',
#                          'HB_temp','TB_temp','holiday_day','count','days_count','ALCSCM','warename')]
# colnames(sale_df_set)[8] = "store_count"
df_month_all = sale_df %>% left_join(sale_df[,.(count=.N), by=uniq_ID],by = 'uniq_ID') %>% data.table()
colnames(df_month_all)[ncol(df_month_all)] = 'TS_count'
### Data Set
holiday_month = read.xlsx("./input_output_file/holiday_update201809.xlsx", 
                          sheetName="day",encoding = 'UTF-8')
holiday_month = holiday_month[,c('date','encode')]
colnames(holiday_month)[2] = 'holiday_day'
holiday_month$date = as.numeric(gsub("-","",holiday_month$date))
forcast_month = holiday_month[holiday_month$date %in% set_forecast_day,]$holiday_day

# train_set = df_month_all[!df_month_all$date %in% c(201807,201808),]
# test_set = df_month_all[df_month_all$date %in% c(201807,201808),]
df_list = split(df_month_all[,c('date','sum_sale_qty','uniq_ID','holiday_day')],df_month_all$uniq_ID)

# Exponential smoothing 
SES_model <- function(forecast_h){
  library(forecast)
  forcast_values = c()
  for(row_j in seq_len(length(df_list))){
    
    
    row_data = df_list[row_j][[1]]$sum_sale_qty
    row_data = df_list[row_j][[1]]$sum_sale_qty
    consumption = ts(row_data,start = c(1),frequency = 1)
    if(length(row_data)>=2){
      model <- as.data.frame(ses(consumption, initial = "optimal", h=forecast_h, beta=NULL, gamma=NULL))
      row_predict = model$`Point Forecast` # h is the no. periods to forecas
    }else{
      row_predict = rep(mean(row_data),forecast_h)
      
    }
    forcast_values = append(forcast_values,row_predict)
    
    cat('row:',row_j,'\n')
    
    
  }
  

  
  forecast_day = rep(set_forecast_day,length(df_list))
  forecast_names = rep(names(df_list),each=forecast_h)
  forecast_df = data.frame(uniq_ID=forecast_names,
                           date=forecast_day,
                           forcast_values = forcast_values,
                           stringsAsFactors=FALSE)
  
  return(forecast_df)
}

# ARIMA
ARIMA_model <- function(forecast_h){
  library(forecast)
  require(smooth)
  require(Mcomp)
  forcast_values = c()
  for(row_j in seq_len(length(df_list))){
    
    row_data = df_list[row_j][[1]]$sum_sale_qty
    row_data = df_list[row_j][[1]]$sum_sale_qty
    consumption = ts(row_data,start = c(1),frequency = 1)
    
    if(length(row_data)>7){

      set.seed(row_j)
      skirtsarima<-auto.arima(consumption,trace=F)
      skirtsarimaforecast<-forecast(skirtsarima,h=forecast_h)
      row_predict = as.data.frame(skirtsarimaforecast)$`Point Forecast`
      
    } else{
      
      if(length(consumption)>=3){
        
        set.seed(row_j)
        model = sma(consumption,order=3, h=forecast_h, silent=FALSE)
        row_predict = as.numeric(model$forecast)
      } else{
        # 
        # set.seed(row_j)
        # model = sma(consumption,order=length(consumption), h=2, silent=FALSE)
        # row_predict = as.numeric(model$forecast)
        row_predict = rep(mean(row_data),forecast_h)
        
      }
      
      
    }
    # predict_7day_df = rbind(predict_7day_df,row_predict$`Point Forecast`)
    forcast_values = append(forcast_values,row_predict)
    cat('row:',row_j,'\n')
    
  }
  
  forecast_day = rep(set_forecast_day,length(df_list))
  forecast_names = rep(names(df_list),each=forecast_h)
  forecast_df = data.frame(uniq_ID=forecast_names,
                           date=forecast_day,
                           forcast_values = forcast_values,
                           stringsAsFactors=FALSE)
  return(forecast_df)
  
}

# XGBoost
XGBoost_model <- function(forecast_h){
  require(smooth)
  require(Mcomp)
  library(forecast)
  library(forecastxgb)
  # source("F:/Sales_Forecast/data_dir/top200/XGBoost/utils.R")
  # source("F:/Sales_Forecast/data_dir/top200/XGBoost/xgbar.R")
  source("/home/test/nby/R_wsp/utils.R")
  source("/home/test/nby/R_wsp/xgbar.R")
  
  forcast_values = c()
  for(row_j in seq_len(length(df_list))){
    
    row_data = df_list[row_j][[1]]$sum_sale_qty
    row_data = df_list[row_j][[1]]$sum_sale_qty
    consumption = ts(row_data,start = c(1),frequency = 1)
    
    if(length(row_data)>=12){
      row_holiday = df_list[row_j][[1]]$holiday_day
      row_holiday_ts = ts(row_holiday,start = c(1),frequency = 1)
      
      
      # df_tqwd = data.frame(row_tianqi=row_tianqi,row_wendu=row_wendu)
      income3 <- matrix(row_holiday, dimnames = list(NULL, "holiday_day"))
      
      set.seed(row_j)
      consumption_model <- xgbar(y = consumption, xreg =  income3)
      
      income_future3 = matrix(forcast_month,dimnames = list(NULL, "holiday_day"))
      
      fore_day = forecast(consumption_model, xreg = income_future3)
      row_predict = as.data.frame(fore_day)$`Point Forecast`
      
    } else if(length(row_data)>7){
      # 
      set.seed(row_j)
      skirtsarima<-auto.arima(consumption,trace=T)
      skirtsarimaforecast<-forecast(skirtsarima,h=forecast_h)
      row_predict = as.data.frame(skirtsarimaforecast)$`Point Forecast`
      
    } else{
      
      if(length(consumption)>=3){
        
        set.seed(row_j)
        model = sma(consumption,order=3, h=forecast_h, silent=FALSE)
        row_predict = as.numeric(model$forecast)
      } else{
        # 
        # set.seed(row_j)
        # model = sma(consumption,order=length(consumption), h=2, silent=FALSE)
        # row_predict = as.numeric(model$forecast)
        row_predict = rep(mean(row_data),forecast_h)
        
      }
      
      
    }
    # predict_7day_df = rbind(predict_7day_df,row_predict$`Point Forecast`)
    forcast_values = append(forcast_values,row_predict)
    cat('row:',row_j,'\n')
    
    
  }

  forecast_day = rep(set_forecast_day,length(df_list))
  forecast_names = rep(names(df_list),each=forecast_h)
  forecast_df = data.frame(uniq_ID=forecast_names,
                           date=forecast_day,
                           forcast_values = forcast_values,
                           stringsAsFactors=FALSE)
  
  return(forecast_df)
}


# Map
SES_forecast = SES_model(set_forecast_day_len)
colnames(SES_forecast)[3] = 'SES_forecast_values'
ARIMA_forecast = ARIMA_model(set_forecast_day_len)
colnames(ARIMA_forecast)[3] = 'ARIMA_forecast_values'
XGB_forecast = XGBoost_model(set_forecast_day_len)
colnames(XGB_forecast)[3] = 'XGB_forecast_values'

add_col = df_month_all[df_month_all$date %in% c(20180801),c('good_gid','uniq_ID','ALCSCM','warename','TS_count')]

df_merge = SES_forecast %>% 
  left_join(ARIMA_forecast,by = c('uniq_ID','date')) %>% 
  left_join(XGB_forecast,by = c('uniq_ID','date')) %>%
  left_join(add_col,by = c('uniq_ID')) %>% data.table()

df_merge$forecast_values = 0.5*df_merge$SES_forecast_values+0.3*df_merge$ARIMA_forecast_values+0.2*df_merge$XGB_forecast_values

df_output = df_merge[,c(6,2,7:10)]
df_output$forecast_values = round(df_output$forecast_values)
OUTPUT_FILE = paste0("./input_output_file/TOP100_modelmerge_forecast_day_",format(Sys.time(), "%Y%m%d%H%M%S"),".xlsx")
write.xlsx(df_output, OUTPUT_FILE,row.names=F, sheetName='modelmerge_day')



