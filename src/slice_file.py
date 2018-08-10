#!/usr/bin/python3
# -*- coding: utf-8 -*-

import sys
import argparse 
import os
import gzip

def main(args):
    if not os.path.exists(args.input):
        print('Input file not exists: ', args.input)
        return

    if not os.path.exists(args.output_dir):
        print('Creating output directory: ', args.output_dir)
        os.makedirs(args.output_dir)

    line_counter = 0

    output_txt_path = os.path.join(args.output_dir, os.path.split(args.input)[-1]+'.txt')
    txt_file = open(output_txt_path, mode='w', encoding='utf-8')
    try:
        with gzip.open(args.input, 'rb') as f:
            for line in f:
                line = line.decode("utf-8") 
                if line.startswith('#'):
                    line_counter -= 1
                line_counter += 1
                txt_file.write(line)
                print('Writing %5d line' % line_counter)
                if line_counter == args.num_line:
                    break
    except Exception as e:
        print(e)
    finally:
        txt_file.close()



def parse_arguments(argv):
    parser = argparse.ArgumentParser()
    parser.add_argument('--input', type=str, 
        default='/home/disk/DYG/1000_genomes/ftp/release/20130502/ALL.wgs.phase3_shapeit2_mvncall_integrated_v5b.20130502.sites.vcf.gz',
        help='input 1kgp file')
    parser.add_argument('--num-line', type=int, default=1000,
        help='Number of lines to save')
    parser.add_argument('--output-dir', type=str, default='./', 
        help='output txt file.')
    parser.add_argument('--verbose', type=bool, help='print processing info.', default=True)
    return parser.parse_args(argv)

if __name__ == '__main__':
    main(parse_arguments(sys.argv[1:]))