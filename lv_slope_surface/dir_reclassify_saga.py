# coding=utf-8
import time
import os


# 可执行文件所在路径
saga_cmd = "/home/beichen/software/opt/saga-7.6.3/bin/saga_cmd"


# 调用SAGA GIS的重分类工具: 原始数据路径 分类后数据路径 分类参照表路径
# 分类表:
# 1、原始流向数据转8制(TauDEM): dir_reclass_table.txt
# 2、原始流向数据128制标准化(pfaf用): d8_normalize.txt
def reclassify_dir(input_file, output_file, table_path):
    print("Dir Reclassify...")
    cmd = saga_cmd + " grid_tools 15 -INPUT " + input_file + " -RESULT " + output_file + " -METHOD 2 -RETAB " + table_path + " -TOPERATOR 1 -RESULT_NODATA_CHOICE 2 -RESULT_NODATA_VALUE 0"
    print(cmd)
    d = os.system(cmd)
    print(d)


if __name__ == '__main__':
    start = time.perf_counter()

    workspace = r"G:\Graduation\Program\Data\36"
    input_tif = workspace + "\\s35e135_dir.tif"
    output_tif = workspace + "\\dir_reclass.tif"
    table = workspace + "\\dir_reclass_table.txt"
    reclassify_dir(input_tif, output_tif, table)

    end = time.perf_counter()
    print('Run', end - start, 's')
