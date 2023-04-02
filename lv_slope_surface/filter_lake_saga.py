# coding=utf-8
import time
import os


# 可执行文件所在路径
saga_cmd = "/home/beichen/software/opt/saga-7.6.3/bin/saga_cmd"


def clip_shp(clip_extent, o_file, result):
    print("Filter Lakes...")
    cmd = saga_cmd + " shapes_polygons 11 -CLIP " + clip_extent + " -S_INPUT " + o_file + " -M_INPUT " + o_file + " -S_OUTPUT " + result + " -M_OUTPUT " + result
    print(cmd)
    d = os.system(cmd)
    print(d)


if __name__ == '__main__':
    start = time.perf_counter()

    workspace = r"G:\Graduation\Program\Figure\5.3_case\demo\slope_surface_lv7\temp"
    basin_extent = workspace + "\\4342161.geojson"
    o_lakes = workspace + "\\lakes_gt_1km2_full.shp"
    output = workspace + "\\lakes.shp"
    clip_shp(basin_extent, o_lakes, output)

    end = time.perf_counter()
    print('Run', end - start, 's')