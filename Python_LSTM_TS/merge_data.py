import pandas as pd
a11 = pd.DataFrame()
for j in range(1,9):
    DataPath = 'F:/xgm_201806/POC/LSTM_20180808/block_output/block'+str(j)+'/'+'block'+str(j)+'_output.csv'
    lstm_output = pd.read_csv(DataPath)
    lstm_output.columns = ['names']+list(lstm_output.columns[1:])
    a11 = a11.append(lstm_output)


a22 = a11.loc[:,['names','sum_7day_true','sum_7day_predict','mape']].rename(columns={'sum_7day_true':'true_total','sum_7day_predict':'forecast_total'})


a22.to_csv('F:/xgm_201806/POC/LSTM_20180808/block_output/lstm_output_all.csv')

a33 = a22['mape'].abs().values

import numpy as np
a33[np.isfinite(a33)].mean()

print(a22.iloc[:5,:])