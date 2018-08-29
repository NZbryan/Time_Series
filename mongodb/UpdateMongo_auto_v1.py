def update_mongo(input_file):
    """
    更新数据
    :param input_file: 
    :return: 
    """
    import pymongo
    client = pymongo.MongoClient(host='10.230.0.125', port=27017)

    # 更新一条信息 X002599_0900003932 的city
    client.python_database.GZ_data_XL_KC.update_one({'names': 'X002599_0900003932'}, {'$set': {'city': 'Guangzhou'}})

    # 所有记录
    all_record = client.python_database.GZ_data_XL_KC.find()

    # 更新大类信息
    coutry_info = {'sname1': '健康美容'}
    for item in all_record:
        _id = item.get('_id')
        client.python_database.GZ_data_XL_KC.update_one({'_id': _id}, {'$set': coutry_info})

    # 新的记录
    new_record = {"name": "john", "sex": "male", "job": "Student11"}
    # 使用 upsert(update+insert), 根据条件判断有无记录，有的话就更新记录，没有的话就插入一条记录
    client.python_database.GZ_data_XL_KC.update_one({'names': 'X002242_0700047341'}, {'$set': new_record}, upsert=True)

    # 删除记录
    client.python_database.GZ_data_XL_KC.update_one({"names": "X002242_0700047341"}, {"$unset": new_record})

    # GZ_data_XL_KC.estimated_document_count()
    # GZ_data_XL_KC.find_one({"names": "X002242_0700047341"})


# update_mongo()


