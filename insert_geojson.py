import os
import json
import pymongo


myclient = pymongo.MongoClient('mongodb://10.4.122.110:27017/')
mydb = myclient["BasinsDataBase_case"]

# for i in range(5, 13):
#     mycol = mydb["Basin_lv" + str(i)]

#     geojson_folder_path = "D:\\Work\\Data_Processing\\basins_geojson\\basins_geojson" + str(i)
#     for root, dirs, files in os.walk(geojson_folder_path):
#         for geojson in files:
#             with open(root + '/' + geojson, 'r') as f:
#                 basin_geoj = json.load(f)
#                 x = mycol.insert_one(basin_geoj)
#                 print(x)

mycol = mydb["coll4index"]
geojson_folder_path = r"D:\Temp\20220907\nested\lv5"
for root, dirs, files in os.walk(geojson_folder_path):
    for geojson in files:
        with open(root + '/' + geojson, 'r') as f:
            basin_geoj = json.load(f)
            x = mycol.insert_one(basin_geoj)
            print(x)