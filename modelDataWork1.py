#!/usr/bin/env python

# inst: university of bristol
# auth: jeison sosa
# mail: j.sosa@bristol.ac.uk / sosa.jeison@gmail.com

import os
import lfptools as lfp
from glob import glob
from shutil import copyfile
from subprocess import call
import gdalutils
import sys
import xarray as xr
import geopandas as gpd


def clip_raster(input_fp, region_fp, output_fp):
    """
    shp裁剪DEM范围
    :param input_fp: DEM文件路径
    :param region_fp: shp路径
    :param output_fp: DEM输出路径
    :return: 输出裁剪后的DEM到lisfloof-fp模拟主目录中
    """
    xds = xr.open_rasterio(input_fp)
    region_gdf = gpd.read_file(region_fp)
    clipped = xds.rio.clip(region_gdf.geometry)
    clipped.rio.to_raster(output_fp)

def geojson2Shapefile(file_path):
    """

    :param file_path: 文件存放位置
    :return:
    """
    try:
        data = gpd.read_file(file_path)
        data.to_file(file_path, driver='ESRI Shapefile', encoding='utf-8')
    except Exception as ex:
        print("--------JSON文件不存在，请检查后重试！----")
        pass

def workprocess(workpath, rainvol,date_begin,date_end):
    """
    文件目录
    workpath：lisfloof-fp模拟主目录 存放DEM裁剪完成的数据、河道宽度数据
        prepdata：lfp.perpdata生成的perp数据
            Basin：生成模型运行所需文件
                lfptools：
                lisfloodfp：lisfloof-fp调用的文件目录
    :param workpath:
           rainvol:雨量大小
           date_begin：模拟开始时间
           date_end：模拟结束时间
    :return:
    """
    if not os.path.exists(workpath):
        os.makedirs(workpath)
    if not os.path.exists(workpath + '/prepdata'):
        os.makedirs(workpath + '/prepdata')
    # 输入shp或者geojson数据裁剪  需要修改
    geojson2Shapefile(workpath)  # 改成geojson地址
    clip_raster(workpath, workpath, workpath + '/DEMtest.tif')  # 输入DEM地址 shp地址  裁剪后的DEM地址
    # 使用TauDEM工具，DEM数据已经计算好了流量和流向数据
    # DEM是裁剪过后的
    call(["PitRemove", "-z", workpath + '/DEMtest.tif', "-fel", workpath + '/DEMtest_PR.tif'])
    call(["D8FlowDir", "-fel", workpath + '/DEMtest_PR.tif', "-p", workpath + '/prepdata/D8FlowDir.tif', "-sd8", workpath + '/prepdata/D8_Slope.tif'])
    call(["D8FlowDir", "-p", workpath + '/prepdata/D8FlowDir.tif', "-ad8", workpath + '/prepdata/D8FlowAcc.tif', "-nc"])
    #获取shp范围
    shpfile = gpd.read_file(r'H:\SHP\liukezhao_soilsample.shp')
    shpfile_extent = gpd.GeoDataFrame(shpfile, crs="EPSG:4326")
    # 使用LFPtools-prepdata工具计算
    lfp.prepdata(te=[shpfile_extent[0], shpfile_extent[1], shpfile_extent[2], shpfile_extent[3]],  # 这个地方还得改，
                 out=workpath + '/prepdata',
                 _dem=workpath + '/DEMtest.tif',  #
                 _acc=workpath + '/prepdata/D8FlowAcc.tif',
                 _dir=workpath + '/prepdata/D8FlowDir.tif',
                 nproc='1',
                 thresh=500,
                 streamnet='yes',
                 overwrite='False',
                 acc_area='False')
    # 使用spilt工具生成所需文件
    lfp.split(basnum='all',
              cattif=workpath + '/prepdata/dem3.tif',
              demtif=workpath + '/prepdata/dem3.tif',
              acctif=workpath + '/prepdata/acc3.tif',
              nettif=workpath + '/prepdata/net3.tif',
              wthtif=workpath + '/width1.tif',
              dirtif=workpath + '/prepdata/dir3.tif',
              aretif=workpath + '/prepdata/area3.tif',
              ordtif=workpath + '/prepdata/strn_ord3d8.tif',
              tretxt=workpath + '/prepdata/strn_tree3d8.txt',
              cootxt=workpath + '/prepdata/strn_coord3d8.txt',
              outdir=workpath + '/prepdata')
    # 生成模型运行所需文件
    filepath = workpath + "/prepdata/Basin"
    if not os.path.exists(filepath):
        os.makedirs(filepath)
    dem = filepath + '/dem.tif'
    # Calling lfp-getwidths
    lfp.getwidths(thresh=0.02,
                  output=filepath + '/lfptools/wdt',
                  recf=filepath + '/rec.csv',
                  netf=filepath + '/net.tif',
                  proj='+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs',
                  fwidth=filepath + '/wth.tif')

    # Calling bankelevs
    lfp.getbankelevs(outlier='yes',
                     method='near',
                     hrnodata=-9999,
                     thresh=0.00416,
                     output=filepath + '/lfptools/bnk',
                     netf=filepath + '/net.tif',
                     hrdemf=dem,
                     recf=filepath + '/rec.csv',
                     proj='+proj=longlat + ellps=WGS84 + datum=WGS84 + no_defs')

    # Calling fixelevs
    lfp.fixelevs(method='yamazaki',
                 source=filepath + '/lfptools/bnk.shp',
                 output=filepath + '/lfptools/bnkfix',
                 netf=filepath + '/net.tif',
                 recf=filepath + '/rec.csv',
                 proj='+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs')

    # Calling rasterreample
    # lfp.rasterresample(nproc=4,
    #                    outlier='yes',
    #                    method='mean',
    #                    hrnodata=-9999,
    #                    thresh=0.00416,
    #                    demf=dem,
    #                    netf='./062/062_net.tif',
    #                    output='./062/lfptools/062_dem30.tif')
    demfile = filepath + '/lfptools/dem.tif'
    demdata = gdalutils.get_data(dem)
    demgeo = gdalutils.get_geo(dem)
    demnodata = demgeo[11]
    gdalutils.write_raster(demdata, demfile, demgeo, "Float32", demnodata)

    # Calling getdepths
    lfp.getdepths(proj='+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs',
                  netf=filepath + '/net.tif',
                  method='depth_geometry',
                  output=filepath + '/lfptools/dpt',
                  wdtf=filepath + '/lfptools/wdt.shp',
                  r=0.12,
                  p=0.78)

    # Calling bedelevs
    lfp.getbedelevs(bnkf=filepath + '/lfptools/bnkfix.shp',
                    dptf=filepath + '/lfptools/dpt.shp',
                    netf=filepath + '/net.tif',
                    output=filepath + '/lfptools/bed',
                    proj='+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs')

    # Creating a folder to save LISFLOOD-FP files
    try:
        os.makedirs(filepath + '/lisfloodfp/')
    except FileExistsError:
        pass

    # Calling buildmodel
    lfp.buildmodel(parlfp=filepath + '/lisfloodfp/test.par',
                   bcilfp=filepath + '/lisfloodfp/test.bci',
                   bdylfp=filepath + '/lisfloodfp/test.bdy',
                   evaplfp=filepath + '/lisfloodfp/test.evap',
                   gaugelfp=filepath + '/lisfloodfp/test.gauge',
                   stagelfp=filepath + '/lisfloodfp/test.stage',
                   rainlfp=filepath + '/lisfloodfp/test.rain',
                   rainvalue=rainvol,
                   dembnktif=filepath + '/lisfloodfp/dembnk.tif',
                   dembnktif_1D=filepath + '/lisfloodfp/dembnk_1D.tif',
                   bedtif=filepath + '/lfptools/bed.tif',
                   wdttif=filepath + '/lfptools/wdt.tif',
                   # runcsv=filepath + '/062_dis.csv',
                   demtif=filepath + '/lfptools/dem.tif',
                   fixbnktif=filepath + '/lfptools/bnkfix.tif',
                   dirtif=filepath + '/dir.tif',
                   reccsv=filepath + '/rec.csv',
                   date1='1998-03-01-00:00',  # 改为带小时分秒的
                   date2='1998-05-01-00:00')
    [copyfile(i, filepath + '/lisfloodfp/' + os.path.basename(i)) for i in glob(filepath + '/lfptools/*.asc')]


def lisFlood_Run(workspace, param1, param2, param3):
    workprocess(workspace, param1, param2, param3)


if __name__ == '__main__':
    path = sys.argv[1]  # 改
    rainvol = sys.argv[2]
    date_begin = sys.argv[3]
    date_end = sys.argv[4]
    workprocess(path, rainvol, date_begin, date_end)
