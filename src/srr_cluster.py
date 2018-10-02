import sys
import os
import itertools
import pandas as pd 
import numpy as np
import pickle 

def get_files(txt_path, path_prefix='/local/yangjing/retrieve/A_to_I_34man/srr/A_to_I.res.'):
    assert os.path.exists(txt_path), 'txt_path not exists: ' + txt_path
    file_paths, column_names = [], []
    with open(txt_path, mode = 'r', encoding='gbk') as f:
        for line in f:
            line_spli = line.strip().split()
            if len(line_spli)>= 2:
                srr = line_spli[0]
                tissu = ' '.join(line_spli[:2])
                path = path_prefix + srr
                if os.path.exists(path):
                    file_paths.append(path)
                    column_names.append(tissu)

    return file_paths, column_names

def get_distance_matrix(file_paths)
    nrof_file = len(file_paths) 
    set_li = [set() for srr in range(nrof_file)]
    for file_i,set_i in zip(file_paths,set_li):
        if not os.path.exists(file_i):
            print("SRR file is not exists: " + file_i)
            continue
        with open(file_i, 'r') as f:
            for line in f:
                line_spli = line.strip().split()
                assert len(line_spli) > 4, 'split error: ' + line
                cols_4 = line_spli[0:4]
                set_i.add(' '.join(cols_4))

    distance_matrix = np.zeros((nrof_file, nrof_file))
    for i in range(nrof_file):
        for j in range(nrof_file):
           # print(len(set_i),len(set_j))
            set_i, set_j = set_li[i], set_li[j]
            if not set_i and not set_j:
                print('set_%s and set_%s are both empty set.'%(i, j))
                continue
            set_and = len(set_i&set_j)
            set_or = len(set_i|set_j)
            like = set_and / set_or
            distance = 1 - like
            distance_matrix[i, j] = distance
    return distance_matrix


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('-i', '--input-txt', type=str, default='./a_human_repeat4_clust.txt')
    parser.add_argument('-o', '--output-dir', type=str, default='./output')
    args = parser.parse_args()
    assert os.path.exists(args.input_txt), 'File not exists: ' + args.input_txt

    if not os.path.exists(args.output_dir):
        os.makedirs(args.output_dir)
    file_paths, column_names = get_files(args.input_txt)

    dist_pickle = os.path.join(arg.output_dir, 'dist.pkl')
    if not os.path.exists(dist_pickle):
        dist = get_distance_matrix(file_paths)
        with open(dist_pickle, 'wb') as f:
            pickle.dump(dist, f)
        print('Saving to ', dist_pickle)
    else:
        with open(dist_pickle, 'rb') as f:
            dist = pickle.load(f)
        print('Loading dist matrix from ', dist_pickle)
    dist_df = pd.DataFrame(dist, index=columns_names, columns=columns_names)
    print('Distane matrix: ')
    print(dist_df.head())

    dist_excel = os.path.join(args.output_dir, 'distance_matrix.xlsx')
    dist_df.to_excel(dist_excel)
    print('Saving to ', dist_excel)

    dist_pairs = []

    N = dist.shape[0]

    for i in range(N):
        for j in range(N):
            if i < j:
                d = dist[i, j]
                pair = [column_names[i], column_names[j], d]
                dist_pairs.append(pair)

    dist_pairs = sorted(dist_pairs, key=lambda x: x[-1]) ### sort pairs based on distance
    dist_pairs_df = pd.DataFrame(dist_pairs, index=None, columns=['Pair_1', 'Pair_2', 'Distance'])
    pairs_excel = os.path.join(args.output_dir, 'pairs_dist.xlsx')
    dist_pairs_df.to_excel(pairs_excel)

