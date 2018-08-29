#!/usr/bin/python3
### python 3.5
# -*- coding: utf-8 -*-

import sys
import argparse 
import os
import pandas as pd
import numpy as np
import itertools


def gen_key_from_excel(excel_path, nrof_sheet=3, selected_col_names=['基因', 'Chrom:Pos Ref/Alt'], print_n_key=3):
    assert os.path.exists(excel_path), 'File not exists: ' + excel_path
    print('Generating key for file: ', excel_path)
    sheets_dict = pd.read_excel(excel_path, sheet_name=None, header=0)
    sheet_names = list(sheets_dict.keys())[:nrof_sheet]
    if len(sheet_names) != nrof_sheet:
        print('WARN: Only found %2d sheets in excel: %s' %(len(sheet_names), excel_path))
    sheets_chosen = [sheets_dict[name] for name in sheet_names]
    nrof_col = len(selected_col_names)
    keys_set = set()
    for sheet in sheets_chosen:
        col_names = list(sheet.columns)
        for name in selected_col_names:
            assert name in col_names, 'Column name %s not exists in sheet with names= %s'%(name, col_names)
        cols_chosen = [list(sheet[name]) for name in selected_col_names]
        cols_chosen = [np.asarray(col).reshape(-1, 1) for col in cols_chosen]
        cols_chosen = np.hstack(cols_chosen)
        keys = set([tuple(columns) for columns in cols_chosen])
        keys_set = keys | keys_set
    if len(keys_set) > 0:
        print('\n'.join([','.join(d) for d in list(keys_set)[:print_n_key]]))
    print('Found %8d key...'%len(keys_set))
    return keys_set

def write_txt(data, txt_path):
    print('data:', data)
    with open(txt_path, 'w', encoding='utf-8') as f:
        lines = [', '.join(k) for k in data]
        f.write('\n'.join(lines))
    print('Saving to ', txt_path)  


def process_pairs(key_pair):
    key_intersect = None
    for key in key_pair:
        if key_intersect is None:
            key_intersect = key
        else:
            key_intersect = key_intersect & key
    return key_intersect

def main(args):
    if not os.path.exists(args.input_dir):
        print('Input directory not exists: ', args.input_dir)
        return

    output_dir = os.path.split(args.output_txt)[0]
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    ### 1. Get all excel files in a directory
    excel_ext = '.xlsx'
    excel_paths = [os.path.join(args.input_dir, p) for p in os.listdir(args.input_dir) if p.endswith(excel_ext)]
    nrof_excel = len(excel_paths)

    assert nrof_excel >= 3, 'Must have at least 3 excel files, found: %d files'%nrof_excel

    ### 2. Generate key for all files
    excel_keys = []
    for path in excel_paths:
        keys = gen_key_from_excel(path, nrof_sheet=3, selected_col_names=['基因', 'Chrom:Pos Ref/Alt'], print_n_key=10)
        excel_keys.append(keys)
    
    ### 3. Get all 3 file pairs
    pair_size = 3
    file_index_pairs= list(itertools.combinations(np.arange(nrof_excel), pair_size))

    ### 4. Pocessing all file-pairs
    total_key = []
    for k, index_pair in enumerate(file_index_pairs):
        key_pair = [excel_keys[i] for i in index_pair]
        keys_combined = process_pairs(key_pair)
        total_key += list(keys_combined)

    write_txt(total_key, args.output_txt) 

    print('All done.')


def parse_arguments(argv):
    parser = argparse.ArgumentParser()
    parser.add_argument('-i', '--input-dir', type=str, required=True, 
        help='Directory contains xlsx files.')
    parser.add_argument('-o', '--output-txt', type=str, default='./output.txt', 
        help='output txt file.')
    parser.add_argument('--verbose', type=bool, help='print processing info.', default=True)
    return parser.parse_args(argv)

if __name__ == '__main__':
    main(parse_arguments(sys.argv[1:]))