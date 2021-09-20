import sys
import time
import os


def create_result_file(o_path):
    time.sleep(3)
    o_path = o_path + '\\c.txt'
    open(o_path, mode='w')


if __name__ == '__main__':
    input1_path = sys.argv[1]
    input2_path = sys.argv[2]
    parameter = int(sys.argv[3])
    output_path = sys.argv[4]
    if os.path.exists(input1_path) and os.path.exists(input2_path):
        if parameter > 0:
            print('Running...')
            print('Parameter: ' + str(parameter))
            create_result_file(output_path)
            print('Over!')
        else:
            print('Bad Parameter!')
    else:
        print('No found!')
    time.sleep(2)
    print('Exit!')
