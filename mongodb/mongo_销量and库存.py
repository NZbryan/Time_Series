import pandas as pd
import os
import numpy as np

## 销量
data_path=r"F:\xgm_201806\POC\LSTM_model_20180731\per_day\test_sales.csv"
df1=pd.read_csv(data_path,index_col=False)
df1['date'] = df1['dt'].apply(lambda x: pd.to_datetime(str(x), format='%Y%m%d'))
df1.index = df1['date']
df1.index.name = 'xiaoliang_date'
df1 = df1['2016-01-01':]
df1['date'] = df1['date'].astype('str')
GZtest_index = list(map(lambda x,y:'X'+x[2:8]+'_'+y[2:12],df1['storecode'],df1['prodcode']))
df1['GZtest_index'] = GZtest_index
GZtest_index = list(set(GZtest_index))

# 库存
df2=pd.read_csv(r'F:\xgm_201806\POC\mongo\collections\test_stock.csv',index_col=False)
kucun_index = list(map(lambda x,y:'X'+x[2:8]+'_'+y[2:12],df2['storecode'],df2['prodcode']))
df2['kucun_index'] = kucun_index
df2['kucun_index_2'] = list(map(lambda x,y:x+'_'+y,df2['date'].tolist(),df2['kucun_index'].tolist()))
# kucun_index = list(set(kucun_index))
# 去重
df_kucun = df2
df_kucun['kucun_qty'] = df_kucun.groupby(['kucun_index_2'])['qty'].transform('sum')
df_kucun = df_kucun.drop_duplicates(['kucun_index_2'])
df_kucun.index = df_kucun['date']
df_kucun = df_kucun.loc[:,['kucun_index','kucun_qty']]
# a11 = list(map(lambda x,y:x+'_'+y,df2['date'].tolist(),df2['kucun_index'].tolist()))
# a22 = list(set(a11))

# data_dict = {}
data_list = []
for k in range(len(GZtest_index)):
    ## 销量
    dat = df1.loc[df1['GZtest_index'] == GZtest_index[k]]
    dat = dat.sort_values(by='date')
    dat = dat.loc[:,['qty','date']]
    dat.index = dat['date'].tolist()
    date_start = dat['date'].tolist()[0]
    date_end = dat['date'].tolist()[-1]
    date_range = pd.date_range(date_start, date_end).astype('str').tolist()
    true_output = pd.DataFrame(index=date_range)
    data_temp = pd.concat([true_output, dat], axis=1, join_axes=[true_output.index])
    # 库存
    dat_kucun_temp = df_kucun.loc[df_kucun['kucun_index'] == GZtest_index[k]]
    data_temp_merge = pd.concat([data_temp, dat_kucun_temp], axis=1, join_axes=[data_temp.index])

    # data_dict[GZtest_index[k]] = {'qty':dat['qty'].tolist(),'date':dat['date'].astype('str').tolist()}
    # 0-90: 1 , 90- 180:2 ,  180--:3
    data_list.append({'names':GZtest_index[k],
                      'date': data_temp_merge.index.tolist(),
                      'xiaoliang_qty':data_temp_merge['qty'].tolist(),
                      'kucun_qty':data_temp_merge['kucun_qty'].tolist()})
                      'data_length':
                      'data_level':

# import pymongo
# client = pymongo.MongoClient(host='10.230.0.125', port=27017)
# db = client.python_database
# GZ_data_XL_KC = db.GZ_data_XL_KC
# result_GZtest_data = GZ_data_XL_KC.insert_many(data_list)
# result_GZtest_data.inserted_ids
# GZ_data_XL_KC.estimated_document_count()
#
# GZ_data_XL_KC.find_one({"names":"X002599_0900003932"})

array11 = list(GZ_data_XL_KC.find())

db.GZ_data_XL_KC.update({},{"$set" : {"daxiao" : [1,2,3]}});



{
    'names':'X002599_0900003932'
    'qty':[1,5,6,..]
    'date':['2016-01-01','2017-1-1',...]
    '':
}


