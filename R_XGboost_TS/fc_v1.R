args<-commandArgs(TRUE)
library(forecastxgb)
# model_path = 'F:/Sales_Forecast/data_dir/top200/XGBoost/forecast_m/ware_model/0100027961_04.rds'
model_path = args[1]
model = readRDS(model_path)
fc <- forecast(model, h = 6)
row_predict = as.data.frame(fc)
cat(row_predict$`Point Forecast`)
