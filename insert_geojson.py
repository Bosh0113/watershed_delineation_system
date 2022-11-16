import os
import json
import pymongo

geojson_folder = r'D:\Work_PhD\MISR_AHI_WS\221115\nested'

myclient = pymongo.MongoClient('mongodb://10.4.122.110:27017/')
mydb = myclient["BasinsDataBase_case"]

for i in range(5, 13):
    mycol = mydb["Basin_lv" + str(i)]

    geojson_folder_path = os.path.join(geojson_folder, 'lv' + str(i))
    for root, dirs, files in os.walk(geojson_folder_path):
        for geojson in files:
            with open(root + '/' + geojson, 'r') as f:
                basin_geoj = json.load(f)
                x = mycol.insert_one(basin_geoj)
                print(x)