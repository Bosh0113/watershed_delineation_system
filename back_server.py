from flask import Flask, render_template, request
from werkzeug.utils import secure_filename
import pymongo
import os
import time
import shutil


app = Flask(__name__)
myclient = pymongo.MongoClient('mongodb://localhost:27017/')
mydb = myclient["BasinsDataBase"]
mycol = mydb["wholeBasinScale"]
app.config['UPLOAD_FOLDER'] = 'static/compute'
app.config['RESULT_FOLDER'] = 'static/results'


@app.route('/main')
def mainpage():
    return app.send_static_file("main.html")


@app.route('/basins/queryScope', methods=['GET'])
def queryScope():
    lon = float(request.args.get('lon'))
    lat = float(request.args.get('lat'))
    clickQuery = {"features.geometry":{"$geoIntersects":{"$geometry":{"type": "Point","coordinates": [lon, lat]}}}}
    geoJson = mycol.find_one(clickQuery)
    if geoJson != None:
        del geoJson["_id"]
        return geoJson
    else:
        return '0'


@app.route('/runCustomData', methods=['POST'])
def runCustomData():
    f_dem = request.files['fileDEM']
    f_lake = request.files['fileLake']
    threshold = request.form.get('threshold')
    
    run_id = str(time.time())
    comp_folder = os.path.join(os.path.abspath('.'), app.config['UPLOAD_FOLDER'], run_id)
    os.makedirs(comp_folder)

    f_dem_path = os.path.join(comp_folder,secure_filename(f_dem.filename))
    f_lake_path = os.path.join(comp_folder,secure_filename(f_lake.filename))

    f_dem.save(f_dem_path)
    f_lake.save(f_lake_path)
    
    resu_folder = os.path.join(os.path.abspath('.'), app.config['RESULT_FOLDER'], run_id)
    os.makedirs(resu_folder)

    cmd = 'python custom_produce.py ' + f_dem_path + ' ' + f_lake_path + ' ' + threshold + ' ' + resu_folder
    d = os.system(cmd)
    print("CMD status", d)

    shutil.make_archive(resu_folder, 'zip', resu_folder)

    shutil.rmtree(comp_folder)
    shutil.rmtree(resu_folder)

    result_zip = "/" + app.config['RESULT_FOLDER'] + "/" + run_id + '.zip'

    return result_zip


if __name__ == "__main__":
    app.run()