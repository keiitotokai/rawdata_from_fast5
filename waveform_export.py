import h5py
import pandas as pd
import numpy as np
import glob
import os
import sys
from tqdm import tqdm
#import time

'''
file直下のGroupは各read
各read直下のGroup：
    ・Analyses > Segmentaion_000 > Summary > segmentaion (Dataset) <<< 1, 2行目合計-3行目がsignalの数
    ・Raw > Signal(Dataset)
    ・channel_id
    ・context_tags
    ・tracking_id
'''


def rawdata_export(folder, threshold):
    #取得データフォルダ内の.fast5を探索
    fast5_list = glob.glob(os.path.join(folder, '**/*.fast5'), recursive=True)

    #全体の出力先ディレクトリ作成
    output_all_dir = str(folder) + '_output'
    os.makedirs(output_all_dir, exist_ok=True)


    for fast5 in fast5_list:
        #ファイルごとの出力先ディレクトリ作成
        output_fast5_dir_path = os.path.join(output_all_dir, fast5)
        os.makedirs(output_fast5_dir_path, exist_ok=True)

        with h5py.File(fast5, 'r') as f:
            #fast5ファイルについてSignal Datasetの値をcsv出力
            for read in tqdm(f):
                try:
                    read_array = np.array(f[read]['Raw']['Signal'])
                    np.savetxt(os.path.join(output_fast5_dir_path, read + '.csv'), read_array, delimiter=',')
                    
                    #temp = read + ' : OK'
                    #print(temp)

                except Exception:
                    temp = read + ' : MISS!'
                    print(temp)

        temp2 = fast5 + ' : Finised'
        print(temp2)



if __name__ == '__main__':
    folder = sys.argv[1]
    #threshold = sys.argv[2]
    rawdata_export(folder, threshold=0)
