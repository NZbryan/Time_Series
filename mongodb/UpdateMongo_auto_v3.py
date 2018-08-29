#!/usr/bin/env python3

def update_mongo():
    """
    更新数据
    :param input_file: 
    :return: 
    """
    import pandas as pd
    # store_info_file = 'F:/xgm_201806/POC/mongo/collections/store_lat_lon.txt'
    store_info_file = '/home/test/public_data_test/store_lat_lon.txt'
    store_info=pd.read_csv(store_info_file)
    store_info['match_id'] = list(map(lambda x:'X'+x,store_info['门店代码']))
    update_dict = {}
    for k in range(store_info.shape[0]):
        update_dict[store_info.iloc[k,:]['match_id']] = {'城市':store_info.iloc[k,:]['城市'],
                                                         '门店gid':str(store_info.iloc[k,:]['门店gid']),
                                                         '门店状态':store_info.iloc[k,:]['门店状态']}
    import pymongo
    client = pymongo.MongoClient(host='10.230.0.125', port=27017)
    # 所有记录
    all_record = client.python_database.all_data_in_test_v1.find()
    # coutry_info = {'sname1': '健康美容'}
    exit_list = []
    for item in all_record:
        store_id = item.get('store_id')
        if store_id not in exit_list:
            exit_list.append(store_id)
            exit_list = list(set(exit_list))
            try:
                add_info = update_dict[store_id]
                # client.python_database.all_data_in_test_v1.update({'store_id': store_id}, {'$set': add_info}, multi=True)
                client.python_database.all_data_in_test_v1.update_many({'store_id': store_id}, {'$set': add_info})

                print('finish:',store_id)
            except:
                print('mismatch:',store_id)


update_mongo()


