
export PATH=/home/test/nby/mongodb/bin:$PATH
export PATH


dbpath=/home/test/nby/mongodb/db
logpath=/home/test/nby/mongodb/logs/mongodb.log
port=27017
fork=true
nohttpinterface=true
security:
   authorization: enabled



#GZtest_data

use miniso_db
db.auth("miniso_user","miniso123")

show collections
db.GZtest_data.find()
db.GZtest_data.find({"grades.grade": "B"})

mongoimport --username miniso_user --password miniso123  --db miniso_db --collection GZtest_data --drop --file /home/test/nby/data_dir/test_sales_data.json

db.restaurants.find( { "borough": "Manhattan" } )


mongoimport --username miniso_user --password miniso123  --db miniso_db --collection GZtest_data --drop --file /home/test/nby/data_dir/dataset.json


mongoimport --username miniso_user --password miniso123  --db miniso_db --collection GZtest_data3 --drop --file /home/test/nby/data_dir/test_sales_data3.txt
