# coding=utf-8
import gdal
import ogr
import osr


# 栅格转矢量多边形(shp): 栅格数据路径 矢量数据路径
def polygonize_to_shp(tif_path, shp_path):
    tif_ds = gdal.Open(tif_path)
    srcband = tif_ds.GetRasterBand(1)
    maskband = srcband.GetMaskBand()
    drv = ogr.GetDriverByName('ESRI Shapefile')
    shp_ds = drv.CreateDataSource(shp_path)
    project = tif_ds.GetProjection()
    srs = osr.SpatialReference(wkt=project)
    dst_layername = 'out'
    dst_layer = shp_ds.CreateLayer(dst_layername, srs=srs)
    dst_fieldname = 'DN'
    fd = ogr.FieldDefn(dst_fieldname, ogr.OFTInteger)
    dst_layer.CreateField(fd)
    gdal.Polygonize(srcband, maskband, dst_layer, 0)


# 栅格转矢量多边形(geojson): 栅格数据路径 矢量数据路径
def polygonize_to_geojson(tif_path, geoj_path):
    tif_ds = gdal.Open(tif_path)
    srcband = tif_ds.GetRasterBand(1)
    maskband = srcband.GetMaskBand()
    drv = ogr.GetDriverByName('GeoJSON')
    geoj_ds = drv.CreateDataSource(geoj_path)
    project = tif_ds.GetProjection()
    srs = osr.SpatialReference(wkt=project)
    dst_layername = 'out'
    dst_layer = geoj_ds.CreateLayer(dst_layername, srs=srs)
    dst_fieldname = 'DN'
    fd = ogr.FieldDefn(dst_fieldname, ogr.OFTInteger)
    dst_layer.CreateField(fd)
    gdal.Polygonize(srcband, maskband, dst_layer, 0)


if __name__ == '__main__':
    workspace = "D:/Graduation/Program/Data/27"
    raster_path = workspace + "/preprocess/watershed.tif"
    vector_path = workspace + "/result/ws.shp"
    polygonize_to_shp(raster_path, vector_path)
    vector_path = workspace + "/result/ws.geojson"
    polygonize_to_geojson(raster_path, vector_path)
