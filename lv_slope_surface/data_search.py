# coding=utf-8
import geopyspark as gps
import json
from pyspark import SparkContext
from shapely.geometry import MultiPolygon
from shapely.geometry import Polygon


# 查询区域内数据: 数据目录路径 范围路径(GeoJSON) dem结果路径 流向结果路径 汇流累积量结果路径
def data_search(catalog_path, json_path, dem_tif_path, dir_tif_path, acc_tif_path):
	catalog_path = "file://" + catalog_path
	# conf = gps.geopyspark_conf(master="local[*]", appName="master")
	# pysc = SparkContext(conf=conf)

	polys = None
	# Creates a MultiPolygon from Geojson
	with open(json_path) as f:
		js = json.load(f)
		FeatureObj = None
		if js['type'] == 'FeatureCollection':
			FeatureObj = js['features'][0]
		elif js['type'] == 'Feature':
			FeatureObj = js
		if FeatureObj['geometry']['type'] == 'MultiPolygon':
			polygons = FeatureObj['geometry']['coordinates']
			polygons_array = []
			for polygon in polygons:
				input_array = []
				for point in polygon[0]:
					input_array.append(tuple(point))
				polygons_array.append(Polygon(input_array))
			polys = MultiPolygon(polygons_array)
		elif FeatureObj['geometry']['type'] == 'Polygon':
			polygon = FeatureObj['geometry']['coordinates']
			points = polygon[0]
			input_array = []
			for point in points:
				input_array.append(tuple(point))
			polys = Polygon(input_array)

	print("Get DEM")
	tiled_raster_layer = gps.query(uri=catalog_path, layer_name="hydro_dem", layer_zoom=0, query_geom=polys)
	print(tiled_raster_layer.count())
	print(tiled_raster_layer.layer_metadata.extent)
	tiled_raster_layer.save_stitched(dem_tif_path)

	print("Get Direction")
	tiled_raster_layer = gps.query(uri=catalog_path, layer_name="hydro_dir", layer_zoom=0, query_geom=polys)
	print(tiled_raster_layer.count())
	print(tiled_raster_layer.layer_metadata.extent)
	tiled_raster_layer.save_stitched(dir_tif_path)

	print("Get Accumulation")
	tiled_raster_layer = gps.query(uri=catalog_path, layer_name="hydro_acc", layer_zoom=0, query_geom=polys)
	print(tiled_raster_layer.count())
	print(tiled_raster_layer.layer_metadata.extent)
	tiled_raster_layer.save_stitched(acc_tif_path)


if __name__ == '__main__':
	workspace = "/usr/local/large_scale_hydro/Test/1"
	catalog = '/usr/local/large_scale_hydro/catalog'
	geojson_path = '/usr/local/large_scale_hydro/result/polygon.geojson'
	dem_result = workspace + '/dem.tif'
	dir_result = workspace + '/dir.tif'
	acc_result = workspace + '/acc.tif'
	data_search(catalog, geojson_path, dem_result, dir_result, acc_result)
