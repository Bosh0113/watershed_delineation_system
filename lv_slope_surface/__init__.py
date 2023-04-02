# coding=utf-8
import os
import geopyspark as gps
from pyspark import SparkContext
import clip_tif_saga as cts
import pfafstetter_coding as pc
import tool_custom_from_RDD as tcfR
import shutil
import raster_polygonize as rp
import numpy

conf = gps.geopyspark_conf(master="local[*]", appName="master")
pysc = SparkContext(conf=conf)

# 基础数据
total_dem_tif = '/disk1/workspace/20220726/dem.tif'
total_dir_o_tif = '/disk1/workspace/20220726/dir.tif'
total_acc_tif = '/disk1/workspace/20220726/acc.tif'

lakes_area_thresholds = {
    '1': 1000000,
    '2': 1000000,
    '3': 1000000,
    '4': 1000,
    '5': 250,
    '6': 250,
    '7': 10,
    '8': 10,
    '9': 10,
    '10': 2,
    '11': 2,
    '12': 2
}

if __name__ == '__main__':
    ws = '/disk1/workspace/20220729'
    error_basin_record = []
    error_basin_filename = ws + '/error_basin_record0812.npy'

    for basin_lv in range(7, 13):
        basin_lv = str(basin_lv)

        basins_folder = ws + '/nested/lv' + basin_lv

        slope_lake_folder = ws + '/slope_lake'
        slope_surface_folder = slope_lake_folder + '/lv' + basin_lv + '/slope_surface'
        lake_folder = slope_lake_folder + '/lv' + basin_lv + '/lake'
        basin_folder = slope_lake_folder + '/lv' + basin_lv + '/sub_basin'
        if not os.path.exists(slope_surface_folder):
            os.makedirs(slope_surface_folder)
        if not os.path.exists(lake_folder):
            os.makedirs(lake_folder)
        if not os.path.exists(basin_folder):
            os.makedirs(basin_folder)

        basins_geojs = os.listdir(basins_folder)
        # 遍历含有湖泊/水库的流域单元
        for basins_geoj in basins_geojs:
            basins_geoj_path = basins_folder + '/' + basins_geoj
            current_path = os.path.abspath(os.path.dirname(__file__))
            temp_folder = current_path + '/temp'
            if not os.path.exists(temp_folder):
                os.makedirs(temp_folder)

            # 获取范围内的数据
            dem_tif = temp_folder + '/dem_p.tif'
            dir_tif = temp_folder + '/dir_p.tif'
            acc_tif = temp_folder + '/acc_p.tif'
            cts.geojson_clip_tif(basins_geoj_path, total_dem_tif, dem_tif)
            cts.geojson_clip_tif(basins_geoj_path, total_dir_o_tif, dir_tif)
            cts.geojson_clip_tif(basins_geoj_path, total_acc_tif, acc_tif)
            # 进行pfafstetter编码次分处理
            river_th = 100.0
            pfaf_1 = temp_folder + '/pfaf_1.tif'
            no_sub_basin = pc.get_pfafstetter_code(dir_tif, acc_tif, pfaf_1, river_th)  # 是否存在次级子流域：1：不存在；0：存在
            # 若没有次级划分
            slope_th = 100.0
            if no_sub_basin:
                slope_th = 100.0
            else:
                slope_th = 2000000.0
            lakes_area_threshold = lakes_area_thresholds[basin_lv]
            # 处理流域内分类
            try:
                basin_info = tcfR.start_main(temp_folder, basins_geoj_path, lakes_area_threshold, river_th, slope_th)
                # 若存在湖泊
                if basin_info:
                        slope_surface_tif = temp_folder + '/result/slope.tif'
                        lake_tif = temp_folder + '/process/lake_revised.tif'
                        sub_basin_tif = temp_folder + '/result/watershed.tif'
                        pfaf_id = basins_geoj.split('.')[0]
                        slope_surface_geoj = slope_surface_folder + '/' + pfaf_id + '.geojson'
                        rp.polygonize_to_geojson(slope_surface_tif, slope_surface_geoj)
                        lake_geoj = lake_folder + '/' + pfaf_id + '.geojson'
                        rp.polygonize_to_geojson(lake_tif, lake_geoj)
                        sub_basin_geoj = basin_folder + '/' + pfaf_id + '.geojson'
                        rp.polygonize_to_geojson(sub_basin_tif, sub_basin_geoj)
                else:
                    shutil.copy(basins_geoj_path, basin_folder + "/" + basins_geoj)
            except Exception as e:
                print(e)
                error_basin_record.append(basins_geoj_path)
            # 删除临时文件夹
            shutil.rmtree(temp_folder)
            # 存储处理过程有问题的流域
            numpy.save(error_basin_filename, error_basin_record)