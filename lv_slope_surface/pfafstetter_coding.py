# coding=utf-8
import time
import rasterio
import pyflwdir     # 0.4.3
from rasterio import crs


# 获取pfafstetter编码的子流域结果: 流向(128制)数据路径 汇流累积量数据路径 结果文件路径 水系提取阈值 pfaf级别(可选，默认次级)
def get_pfafstetter_code(dir_d8_path, acc_path, pfaf_path, stream_th, level=1):
    with rasterio.open(dir_d8_path, 'r') as src:
        flwdir = src.read(1)
        transform = src.transform
        latlon = True

        flw = pyflwdir.from_array(flwdir, ftype='d8', transform=transform, latlon=latlon)

        # upstream_cells = flw.upstream_area()
        with rasterio.open(acc_path, 'r') as acc_f:
            upstream_cells = acc_f.read(1)
            crs_r = crs.CRS.from_wkt('GEOGCS["WGS 84",DATUM["WGS_1984",SPHEROID["WGS 84",6378137,298.257223563,'
                                   'AUTHORITY["EPSG","7030"]],AUTHORITY["EPSG","6326"]],PRIMEM["Greenwich",0,'
                                   'AUTHORITY["EPSG","8901"]],UNIT["degree",0.01745329251994328,AUTHORITY["EPSG",'
                                   '"9122"]],AUTHORITY["EPSG","4326"]]')
            pfaf = flw.pfafstetter(level, upstream_cells, stream_th)
            # 判断是否有次分流域
            no_basin = 1
            for i in range(len(pfaf)):
                if no_basin:
                    for j in range(len(pfaf[0])):
                        value = pfaf[i][j]
                        if value > 0:
                            no_basin = 0
                            break

            print(no_basin)
            # 若没有次分结果则直接返回
            if no_basin:
                return no_basin

            profile = src.profile
            profile.update(dtype=rasterio.uint32, count=1, compress='deflate', crs=crs_r)
            with rasterio.open(pfaf_path, 'w',  **profile) as dst:
                dst.write(pfaf, 1)
            return no_basin


if __name__ == '__main__':
    start = time.perf_counter()

    workspace = r'G:\Graduation\Program\Data\38'
    dir_tif_path = workspace + '/data/dir_d8.tif'
    acc_tif_path = workspace + '/data/acc.tif'
    stream_th_value = 740.0
    pfaf_level = 1
    pfaf_tif_path = workspace + '/test_one_river/pfaf_' + str(pfaf_level) + '.tif'

    get_pfafstetter_code(dir_tif_path, acc_tif_path, pfaf_tif_path, stream_th_value, pfaf_level)

    end = time.perf_counter()
    print('Run', end - start, 's')

