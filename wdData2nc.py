import subprocess
from glob import glob
import os
import pandas as pd
import xarray as xr
import datetime
import sys

def time_index_from_filenames(filenames):
    '''
    helper function to create a pandas DatetimeIndex
       Filename example: 20150520013000.tif
    '''
    # filename=[os.path.basename(filenames) for f in filenames]
    return pd.DatetimeIndex([pd.Timestamp(os.path.basename(f)[:14]) for f in filenames])

def wd2nc(filepath,times):
    """lisflood-fp生成的是.wd的二进制文件
    filepath/wd/*.wd --> filepath/tif/*.tif --> filepath/nc/.nc

    Args:
        filepath (str): 存放.wd文件的上一级目录
        times (时间序列): 从前端获取的模拟时间段
    """
    wdFiles = sorted(glob(filepath + '/wd/*.wd'))
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
        subprocess.call(["gdal_translate", "-of", fmt1,"-a_srs", "EPSG:4326", wdFiles[i], tiffile])
        subprocess.call(["gdal_translate", "-of", fmt2, tiffile, ncfile])

    return()

def nc2ncs(filepath):
    """多个nc文件用时间维度连接
    variabl名称Band1
    Args:
        filepath (str): 路径（同[wd2nc]）
    """
    filenames = glob(filepath + '/nc/*.nc')
    time = xr.Variable('time', time_index_from_filenames(filenames))
    da = xr.concat([xr.open_dataset(f) for f in filenames], dim=time)
    da.to_netcdf(filepath + '/' + 'ncresult2.nc')

if __name__ == '__main__':
    path = sys.argv[1]
    times = sys.argv[2]
    #times = pd.date_range(start='2019-1-09',periods=11,freq='H')
    #path = r'D:/Lisflood-fd/lisflood-fp/results'
    wd2nc(path,times)
    nc2ncs(path)
