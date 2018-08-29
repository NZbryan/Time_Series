import pandas as pd
import os
import numpy as np
## set data path
data_path=r"F:\xgm_201806\POC\LSTM_model_20180731\per_day\test_sales.csv"
df1=pd.read_csv(data_path,index_col=False)
df1['date'] = df1['dt'].apply(lambda x: pd.to_datetime(str(x), format='%Y%m%d'))
df1['date'] = df1['date'].astype('str')
GZtest_index = list(map(lambda x,y:'X'+x[2:8]+'_'+y[2:12],df1['storecode'],df1['prodcode']))
df1['GZtest_index'] = GZtest_index
GZtest_index = list(set(GZtest_index))
data_dict = {}
data_list = []
for k in range(len(GZtest_index)):
    dat = df1.loc[df1['GZtest_index'] == GZtest_index[k]]
    dat = dat.sort_values(by='date')
    # data_dict[GZtest_index[k]] = {'qty':dat['qty'].tolist(),'date':dat['date'].astype('str').tolist()}
    data_list.append({'names':GZtest_index[k],'qty':dat['qty'].tolist(),'date':dat['date'].astype('str').tolist()})


import pymongo
client = pymongo.MongoClient(host='10.230.0.125', port=27017)
db = client.python_database
# GZtest_data = db.GZtest_data
# result_GZtest_data = GZtest_data.insert_many(data_list)
# result_GZtest_data.inserted_ids
GZtest_data.estimated_document_count()

GZtest_data.find_one({"names":"X002599_0900003932"})

k11 = data_dict[GZtest_index[0]]

# array = list(GZtest_data.find())

GZtest_data.find({"names":"X002599_0900003932"},{"qty":{"$gte":10}})

db.spam

{
    'names':'X002599_0900003932'
    'qty':[1,5,6,..]
    'date':['2016-01-01','2017-1-1',...]
    '':

}




for data in GZtest_data.find():
    print(data)

uni_names


for u1 in GZtest_index:
    a11 = GZtest_data.find_one({"names":u1})
    # print(a11)


GZtest_data.find_one({'_id':'5b67b05202aa0027fc45c701'})

