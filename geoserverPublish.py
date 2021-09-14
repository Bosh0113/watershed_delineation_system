#!/usr/bin/env python
# -*- coding: utf-8 -*-
from geo.Geoserver import Geoserver
from geoserver.workspace import Workspace
import sys

#geserver配置
geo = Geoserver('http://localhost:8080/geoserver', username='admin', password='geoserver')

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

if __name__ == '__main__':
    image_path = sys.argv[1]
    workspace_name = sys.argv[2]
    geoserver_publish_nc(image_path,workspace_name)



