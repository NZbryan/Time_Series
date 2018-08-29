import subprocess
model_path = 'F:/Sales_Forecast/data_dir/top200/XGBoost/forecast_m/ware_model/0100027961_04.rds'
Rscript_path = 'F:/Sales_Forecast/data_dir/top200/XGBoost/fc_v1.R'
R_path = 'C:/R/R-3.5.0/bin/RScript'
proc = subprocess.Popen([R_path,Rscript_path,model_path], stdout=subprocess.PIPE, stderr=subprocess.PIPE,shell=True)
stdout, stderr = proc.communicate()
stdout = stdout.decode('utf-8').split()
forecast_data = list(map(float, stdout))