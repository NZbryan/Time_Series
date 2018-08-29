#!/usr/bin/env python3
import multiprocessing
import pymongo
import pandas as pd

def update_data():
    # store_info_file = 'F:/xgm_201806/POC/mongo/collections/store_lat_lon.txt'
    store_info_file = '/home/test/public_data_test/store_lat_lon.txt'
    store_info=pd.read_csv(store_info_file)
    store_info['match_id'] = list(map(lambda x:'X'+x,store_info['门店代码']))
    update_dict = {}
    for k in range(store_info.shape[0]):
        update_dict[store_info.iloc[k,:]['match_id']] = {'城市':store_info.iloc[k,:]['城市'],
                                                         '门店gid':str(store_info.iloc[k,:]['门店gid']),
                                                         '门店状态':store_info.iloc[k,:]['门店状态']}
    return update_dict

def process_cursor(skip_n,limit_n):
    print('Starting process',skip_n//limit_n,'...')
    client = pymongo.MongoClient(host='10.230.0.125', port=27017)
    # collection = MongoClient().<db_name>.<collection_name>
    collection = client.python_database.all_data_in_test_v1
    cursor = collection.find({}).skip(skip_n).limit(limit_n)
    update_dict = update_data()
    exit_list = []
    for item in cursor:
        store_id = item.get('store_id')
        if store_id not in exit_list:
            exit_list.append(store_id)
            exit_list = list(set(exit_list))
            try:
                add_info = update_dict[store_id]
                # client.python_database.all_data_in_test_v1.update({'store_id': store_id}, {'$set': add_info}, multi=True)
                client.python_database.all_data_in_test_v1.update_many({'store_id': store_id}, {'$set': add_info})

                print('finish:', store_id)
            except:
                print('mismatch:', store_id)
    print('Completed process',skip_n//limit_n,'...')

if __name__ == '__main__':
    n_cores = 8  # number of splits (logical cores of the CPU-1)
    collection_size = 12894620
    batch_size = round(collection_size / n_cores + 0.5)
    skips = range(0, n_cores * batch_size, batch_size)

    processes = [multiprocessing.Process(target=process_cursor, args=(skip_n, batch_size)) for skip_n in skips]

    for process in processes:
        process.start()

    for process in processes:
        process.join()




