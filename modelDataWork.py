#!/home/nnu/anaconda3/envs/py37/bin python

import os
import lfptools as lfp
from glob import glob
from shutil import copyfile
from subprocess import call
import gdalutils
import sys
import xarray as xr
import geopandas as gpd
import time
import pandas as pd
from geo.Geoserver import Geoserver



# lisflood路径
lisflood_path = r'/home/nnu/files/LISFLOOD-FP-trunk/build'

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
        data.to_file(os.path.dirname(file_path) + '/basin.shp', driver='ESRI Shapefile', encoding='utf-8')
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
    geojson2Shapefile(workpath + '/basin.geojson')  # 改成geojson地址
    # clip_raster(workpath, workpath, workpath + '/DEMtest.tif')  # 输入DEM地址 shp地址  裁剪后的DEM地址
    call(["gdalwarp", "-ot", "Float32", "-crop_to_cutline", "-cutline", workpath + '/basin.shp', "-dstnodata", "-9999", "-co", 'COMPRESS=DEFLATE', "-overwrite",
          "-co", "BIGTIFF=YES", '/home/nnu/web/data/DEM.tif', workpath + '/DEMtest.tif'])
    call(["gdalwarp", "-ot", "Float32", "-crop_to_cutline", "-cutline", workpath + '/basin.shp', "-dstnodata", "-9999", "-co", 'COMPRESS=DEFLATE', "-overwrite",
          "-co", "BIGTIFF=YES", '/home/nnu/web/data/GWD-LR-cj.tif', workpath + '/width.tif'])
    # 使用TauDEM工具，DEM数据已经计算好了流量和流向数据
    # DEM是裁剪过后的
    call(["pitremove", "-z", workpath + '/DEMtest.tif', "-fel", workpath + '/DEMtest_PR.tif'])
    call(["d8flowdir", "-fel", workpath + '/DEMtest_PR.tif', "-p", workpath + '/prepdata/D8FlowDir.tif', "-sd8", workpath + '/prepdata/D8_Slope.tif'])
    call(["aread8", "-p", workpath + '/prepdata/D8FlowDir.tif', "-ad8", workpath + '/prepdata/D8FlowAcc.tif', "-nc"])
    #时间暂停10
    time.sleep(5)
    #获取shp范围
    shpfile = gpd.read_file(workpath + '/basin.shp')
    shpfile_extent = gpd.GeoDataFrame(shpfile, crs="EPSG:4326")
    # 使用LFPtools-prepdata工具计算
    lfp.prepdata(te=[shpfile_extent.total_bounds[0], shpfile_extent.total_bounds[1], shpfile_extent.total_bounds[2], shpfile_extent.total_bounds[3]],  # 这个地方还得改，
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
              wthtif=workpath + '/width.tif',
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
    lfp.buildmodel(workpath=workpath,
                   parlfp=filepath + '/lisfloodfp/test.par',
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
                   date1=date_begin,                       # 改为带小时分秒的
                   date2=date_end)
    [copyfile(i, filepath + '/lisfloodfp/' + os.path.basename(i)) for i in glob(filepath + '/lfptools/*.asc')]

    lisflood_cmd = lisflood_path + "/lisflood" + ' ' + '-v' +  '  ' + workpath + '/prepdata/Basin/lisfloodfp/test.par' 
    os.system(lisflood_cmd)


# wddata2nc
def time_index_from_filenames(filenames):
    '''
    helper function to create a pandas DatetimeIndex
       Filename example: 20150520013000.tif
    '''
    # filename=[os.path.basename(filenames) for f in filenames]
    return pd.DatetimeIndex([pd.Timestamp(os.path.basename(f)[:14]) for f in filenames])

def wd2nc(filepath,times):
    """lisflood-fp生成的是.wd的二进制文件
    filepath/*.wd --> filepath/tif/*.tif --> filepath/nc/.nc

    Args:
        filepath (str): 存放.wd文件的上一级目录
        times (时间序列): 从前端获取的模拟时间段
    """
    print("wd2nc-working")
    wdFiles = sorted(glob(filepath + '/*.wd'))
    #tiff文件存放地址
    tifFilepath = filepath + '/tif'
    ncFilepath = filepath + '/nc'
    if not os.path.exists(tifFilepath):
        os.makedirs(tifFilepath)
    if not os.path.exists(ncFilepath):
        os.makedirs(ncFilepath)

    for i in range(len(wdFiles)):
        time_str = times[i].strftime("%Y%m%d%H%M%S")
        tiffile = tifFilepath + '/' + time_str + '.tif'
        ncfile = ncFilepath + '/' + time_str + '.nc'
        fmt1 = 'GTiff'
        fmt2 = 'netcdf'
        call(["gdal_translate", "-of", fmt1,"-a_srs", "EPSG:4326", wdFiles[i], tiffile])
        call(["gdal_translate", "-of", fmt2, tiffile, ncfile])
        print("wd2nc woring",wdFiles[i])
def nc2ncs(filepath):
    """多个nc文件用时间维度连接
    variabl名称Band1
    Args:
        filepath (str): 路径（同[wd2nc]）
    """
    filenames = glob(filepath + '/nc/*.nc')
    time = xr.Variable('time', time_index_from_filenames(filenames))
    da = xr.concat([xr.open_dataset(f) for f in filenames], dim=time)
    da.to_netcdf(filepath + '/' + 'ncresult.nc')

#geserver配置
geo = Geoserver('http://116.63.252.134:8080/geoserver', username='admin', password='dgpm$321')
def geoserver_publish_nc(image_path,workspace_name):
    """
    在geoserver上数据nc格式的数据

    Args:
        image_path (str): nc文件的路径
        workspace_name (str): 创建工作区间的名字，每次模拟工作区间名字不能一样
    """
    #creat workspace
    geo.create_workspace(workspace_name)
    #upload
    geo.create_coveragestore_nc(workspace=workspace_name,path=image_path,coveragestore_name='Band1')
    #add time 
    geo.publish_time_dimension_to_coveragestore(workspace=workspace_name,store_name='Band1')
    #set style
    geo.create_coveragestyle(raster_path=image_path, style_name='style_1', workspace=workspace_name, color_ramp='RdYlGn')
    geo.publish_style(layer_name='Band1', style_name='style_1', workspace=workspace_name)


def lisFlood_Run(workspace, param1, param2, param3):

    workprocess(workspace, param1, param2, param3)
    
    #
    wdpath = workspace + '/result'
    if not os.path.exists(wdpath):
        os.makedirs(wdpath) 
    print("workprocess--wd2nc-working")
    times = pd.date_range(start=param2,end=param3,freq='30min')
    wd2nc(wdpath,times)
    nc2ncs(wdpath)
    #
    image_path = wdpath + '/' + 'ncresult.nc'
    workspace_name = os.path.basename(workspace)
    geoserver_publish_nc(image_path,workspace_name)
    wmsUrl = "http://116.63.252.134:8080/geoserver/"+ workspace_name +"/wms?"
    return(wmsUrl)
    
if __name__ == '__main__':

    path = sys.argv[1]  # 改
    rainvol = sys.argv[2]
    date_begin = sys.argv[3]
    date_end = sys.argv[4]
    workprocess(path, rainvol, date_begin, date_end)
    # path = sys.argv[1]  # 改
    # rainvol = sys.argv[2]
    # path = r'D:/Lisflood-fd/lisflood-9-13'  # 改
    # rainvol = 50
    # # date_begin = sys.argv[3]
    # # date_end = sys.argv[4]
    # workprocess(path, rainvol, None, None)  # , date_begin, date_end)
