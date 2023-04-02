# coding=utf-8
import os

# 可执行文件所在路径
saga_cmd = "/home/beichen/software/opt/saga-7.6.3/bin/saga_cmd"

# geojson裁剪tif: geojson路径 tif路径 结果路径
def geojson_clip_tif(geojson_path, tif_path, result_path):
    print("Crop Raster by GeoJSON...")
    cmd = saga_cmd + " shapes_grid 7 -INPUT " + tif_path + " -OUTPUT " + result_path + " -POLYGONS " + geojson_path
    print(cmd)
    d = os.system(cmd)
    print(d)


# shapefile裁切tif: shapefile路径 tif路径 结果路径
def shp_clip_tif(shp_path, raster_path, result_path):
    print("Crop Raster by Shp...")
    cmd = saga_cmd + " shapes_grid 7 -INPUT " + raster_path + " -OUTPUT " + result_path + " -POLYGONS " + shp_path
    print(cmd)
    d = os.system(cmd)
    print(d)


if __name__ == '__main__':
    workspace_path = "D:/Graduation/Program/Data/21"
    shp_file = workspace_path + "/query/polygon.shp"
    geoj_file = workspace_path + "/query/polygon.geojson"

    dir_file = workspace_path + "/query/dir.tif"
    dir_mask_file = workspace_path + "/dir_geoj_saga.tif"
    geojson_clip_tif(geoj_file, dir_file, dir_mask_file)

    tif_file = workspace_path + "/query/dir.tif"
    dir_mask_file = workspace_path + "/dir_shp_saga.tif"
    shp_clip_tif(shp_file, tif_file, dir_mask_file)
