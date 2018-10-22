#!/usr/bin/python3
# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
import sys
import time
import argparse
import glob
import numpy as np
from collections import defaultdict
from sklearn.cluster import DBSCAN
from multiprocessing import Process


def select_lines(pairs, min_dist=3, min_samples=2):
    cluster = DBSCAN(eps=1.5, min_samples=min_samples, metric='l1', n_jobs=20)
    pairs = sorted(pairs, key=lambda x: x[0])
    nrof_pair = len(pairs)
    x = np.zeros((nrof_pair, 2), dtype=np.int)
    x[:, 0] = np.array([p[0] for p in pairs])
    y = cluster.fit_predict(x)
    lines_keep = []
    last_dist, next_dist = None, None
    for i, c in enumerate(y):
        current_dist, line = pairs[i]
        if c == -1:
            next_dist = None
            for j in range(i+1, nrof_pair):
                dist, _ = pairs[j]
                cc = y[j]
                if cc != -1:
                    next_dist = dist
                    break
            if last_dist and abs(current_dist - last_dist) < min_dist:
                lines_keep.append(line)
                continue
            if next_dist and abs(current_dist - next_dist) < min_dist:
                lines_keep.append(line)
                continue
        else:
            last_dist = current_dist
            lines_keep.append(line)
    return lines_keep


def process_file(txt_in, output_dir, min_dist=3, min_samples=2):
    _, filename = os.path.split(txt_in)
    print('Start processing file: ', filename)
    time_start = time.time()
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    txt_out = os.path.join(output_dir, filename)

    data_dic = defaultdict(list)
    print('Reading txt file ...')
    with open(txt_in, 'r') as f:
        for line in f.readlines(): ### line end has \n
            line_split = line.strip().split() 
            nrof_cols = len(line_split)
            if nrof_cols < 2:
                print('Warn: split maybe wrong for line: ', line_split)
                continue
            chrom, dist = line_split[:2]
            dist = int(dist)
            pair = (dist, line)
            data_dic[chrom].append(pair)

    print('Do clustering...')
    total_lines = []
    for chrom, pairs in data_dic.items():
        total_lines += select_lines(pairs, min_dist=min_dist, min_samples=min_samples)

    print('Writing txt file ...')
    with open(txt_out, 'w') as f:
        f.writelines(total_lines)
    diff_sec = time.time() - time_start
    print('Time %.3fs, saving to: %s'%(diff_sec, txt_out))


def main(args):
    assert os.path.exists(args.dir_in), 'File not exists: ' + args.dir_in
    txt_paths = glob.glob(os.path.join(args.dir_in, '*' + args.ext))
    nrof_txt = len(txt_paths)
    print('Totally %2d txt in directory: %s' %(nrof_txt, args.dir_in))
    proc_li = []
    for i in range(nrof_txt):
        func_args = (txt_paths[i], args.dir_out, args.min_dist, args.min_samples)
        p = Process(target=process_file, args=func_args)
        proc_li.append(p)

    for p in proc_li:
        p.start()
        p.join()


    print('All done.')


def parse_arguments(argv):
  parser = argparse.ArgumentParser()
  parser.add_argument('-i', '--dir-in', required=True, help='Input directory path')
  parser.add_argument('-o', '--dir-out', default='../output', help='Output directory path')
  parser.add_argument('-e', '--ext', default='.alt', help='File extensions.')
  parser.add_argument('-m', '--min-samples', default=2, help='min samples per cluster.')
  parser.add_argument('-n', '--min-dist', default=3, help='min dist of special samples to cluster.')
  return parser.parse_args(argv)


if __name__ == '__main__':
    main(parse_arguments(sys.argv[1:]))