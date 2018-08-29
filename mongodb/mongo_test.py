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

k11 = data_dict[GZtest_index[0]]


import pymongo
client = pymongo.MongoClient(host='10.230.0.125', port=27017)
db = client.python_database
# db.authenticate("miniso_user","miniso123")
collection = db.test_collection

import datetime
post = {'type': 'BSON','date':datetime.datetime.utcnow()}
document1 = {'x':1}
document2 = {'x':2}
posts = db.posts     #你也可以不这样做，每次通过db.posts调用
post_1 = posts.insert_one(document1).inserted_id
post_2 = posts.insert_one(document2).inserted_id

new_document = [{'x':3},{'x':4}]
result = posts.insert_many(new_document)
result.inserted_ids

posts.find_one()

for data in posts.find():
    print(data)

for post in posts.find({'x':1}):
    print(post)

posts.estimated_document_count()

post_3 = posts.insert_one(k11).inserted_id


GZtest_data = db.GZtest_data
result_GZtest_data = GZtest_data.insert_many(data_list)
result_GZtest_data.inserted_ids
GZtest_data.estimated_document_count()

for data in posts.find():
    print(data)

GZtest_data.find()

GZtest_data.find_one({"names":"X002599_0900003932"},{"qty":{"$gte":10}})

GZtest_data.find({"qty":{"$gte":10}})


GZtest_data.find_one().pretty()

GZtest_data.find()


# db.GZtest_data.find()
# db.GZtest_data.remove({})

for data in GZtest_data.find_one({"qty":{"$gte":10}}):
    print(data)


db.users.find({"names":{"$in":["X002599_0900003932","X000116_0200026781"]}})


for data in GZtest_data.find({"names":["X002599_0900003932","X000116_0200026781"]}):
    print(data)



for k in db.users.find({"names":{"$in":["X002599_0900003932"]}}):
    print(k)