from flask import Flask, render_template, request
import pymongo


app = Flask(__name__)
myclient = pymongo.MongoClient('mongodb://localhost:27017/')
mydb = myclient["BasinsDataBase"]
mycol = mydb["wholeBasinScale"]


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


if __name__ == "__main__":
    app.run()