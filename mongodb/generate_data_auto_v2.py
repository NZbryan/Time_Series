# import numpy as np
def generate_data(input_file):
    import pandas as pd
    import os
    # input_file = r'F:\xgm_201806\POC\mongo\collections\X000760_0300014541.csv'
    df1=pd.read_csv(input_file,index_col=0)
    if len(df1)==0:
        return 0
    # data_level
    dateNotNALen = df1['sale_qty'].notna().sum()
    data_level = 1 if dateNotNALen<=90 else (3 if dateNotNALen>=180 else 2)

    # weekday
    weekday_data = pd.date_range(df1.index.tolist()[0], df1.index.tolist()[-1])
    week_info = list(map(lambda x: x.weekday() + 1, weekday_data))

    data_dict = {'names': os.path.splitext(os.path.basename(input_file))[0],
    'date': df1.index.tolist(),
    'weekday':week_info,
    'sale_qty': df1['sale_qty'].tolist(),
    'stock_qty': df1['stock_qty'].tolist(),
    'fill_sale_qty': df1['fill_sale_qty'].tolist(),
    'data_length': [df1.index[0] + '~' + df1.index[-1], dateNotNALen / df1.shape[0]],
    'data_level': data_level}
    return data_dict


def insert_mongo(input_file):
    import pymongo
    client = pymongo.MongoClient(host='10.230.0.125', port=27017)
    db = client.python_database
    # db.GZ_data_XL_KC.drop()
    GZ_data_XL_KC = db.GZ_data_XL_KC
    output = generate_data(input_file)
    if output!=0:
        GZ_data_XL_KC.insert_one(output)
    # GZ_data_XL_KC.estimated_document_count()
    # GZ_data_XL_KC.find_one({"names": "X000318_0200016551"})['date']

def main():
    import os
    filedir = "/SAS/data_v1/sale_fill_na_v2"
    listdir = os.listdir(filedir) # returns list
    for k in range(len(listdir)):
        input_file=os.path.join(filedir, listdir[k])
        insert_mongo(input_file)

# GZ_data_XL_KC.find_one({"names":"X002417_0200028811"})

if __name__ == '__main__':
    main()
