def update_mongo(input_file):
    """
    更新数据
    :param input_file: 
    :return: 
    """
    import pymongo
    import pandas as pd
    client = pymongo.MongoClient(host='10.230.0.125', port=27017)


    # 所有记录
    all_record = client.python_database.GZ_data_XL_KC.find()
    # coutry_info = {'sname1': '健康美容'}
    for item in all_record:
        date_time = pd.date_range(item['date'][0], item['date'][-1])
        week_info = {"weekday":list(map(lambda x: x.weekday()+1,date_time))}
        names = item.get('names')
        client.python_database.GZ_data_XL_KC.update_one({'names': names}, {'$set': week_info})


# update_mongo()

import pymongo

client = pymongo.MongoClient(host='10.230.0.125', port=27017)
db = client.python_database
# db.GZ_data_XL_KC.drop()
all_data_in_test_v1 = db.all_data_in_test_v1
# output = generate_data(input_file)
all_data_in_test_v1.estimated_document_count()
all_data_in_test_v1.find_one({"names": "X000318_0200016551"})

all_data_in_test_v1.find_one()



all_data_in_test_v1.find_one({"names": "X003643_0100000896"})


城市、商圈、门店GID、门店状态、仓库名称






# all_data_in_test_v1.find_one({"names": "X003643_0100000896"})
#
# list(all_data_in_test_v1.find({"store_id": "X003643"}))
#
# X003643_0200043673
#
#
# update_dict['X003643']
#
# add_info = update_dict['X003643']
#
# client.python_database.all_data_in_test_v1.update_one({'names': "X003643_0200043673"}, {'$set': add_info})
# client.python_database.all_data_in_test_v1.find_one({"names": "X003643_0200043673"})
#
# import pymongo
# client = pymongo.MongoClient(host='10.230.0.125', port=27017)
# client.python_database.all_data_in_test_v1.find({"store_id": "X003643"})
#
# # client.python_database.all_data_in_test_v1.update_one({"names": "X003643_0200043673"}, {"$unset": add_info})
#
#
#
# h11 = list(client.python_database.all_data_in_test_v1.find({"store_id": "X003643"}))
#
#
# client.python_database.all_data_in_test_v1.update_one({"store_id": "X003643"})
#
#
# client.python_database.all_data_in_test_v1.update({'store_id': "X003643"}, {'$set': add_info},multi =True)
#
# client.python_database.all_data_in_test_v1.find_one({"names": "X003643_0100000896"})
# client.python_database.all_data_in_test_v1.find_one({"names": "X003643_0100024001"})

import pymongo

client = pymongo.MongoClient(host='10.230.0.125', port=27017)
db = client.python_database
# db.GZ_data_XL_KC.drop()
all_data_in_test_v1 = db.all_data_in_test_v1
all_record = client.python_database.all_data_in_test_v1.find()
item = all_record[-1]
for item in all_record

client.python_database.all_data_in_test_v1.find_one({'store_id':'X000937'})

p11 = list(client.python_database.all_data_in_test_v1.find({'store_id':'X000318'}))

client.python_database.all_data_in_test_v1.find({'store_id':'X000318'})




client.python_database.all_data_in_test_v1.find_one({'store_id':'X000538'})


client.python_database.all_data_in_test_v1.find({'store_id':'X001560'})
a11 = list(client.python_database.all_data_in_test_v1.find({'store_id':'X001560'}))

# db.all_data_in_test_v1.find().sort({_id:1})
for item in client.python_database.all_data_in_test_v1.find().limit(2).skip(12894620):
    store_id = item.get('store_id')

