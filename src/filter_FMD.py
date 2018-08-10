#!/usr/bin/python3
# -*- coding: utf-8 -*-
# *******************************************************************#
#          ruijin_FMD_filt_1KGP 千人基因组过滤FMD WGS数据脚本           #
# *******************************************************************#

import sys
import argparse 
import os
import gzip
import pickle
from concurrent import futures
from datetime import datetime
import time

def gen_key_stats_from_1kgp(input_gz_file, output_pkl, VT='SNP', print_every=1000):
    if not os.path.exists(input_gz_file):
        print('Input 1KGP file not exists: ', input_gz_file)
        return
    key_set = set()
    num_line_total = 0
    num_line_has_qualified_VT = 0
    try:
        with gzip.open(input_gz_file, 'rb') as f:
            for line in f:
                num_line_total += 1
                line = line.decode("utf-8").strip() 
                if line.startswith('#'):
                    continue
                line_1kgp_split = line.split()
                vt = line_1kgp_split[-1].split('=')[-1]
                if vt != VT:
                    continue
                num_line_has_qualified_VT += 1
                chrom_1kgp, pos_1kgp, ref_1kgp, alt_1kgp = line_1kgp_split[:2] + line_1kgp_split[3:5]
                key = (chrom_1kgp, pos_1kgp, ref_1kgp, alt_1kgp)
                if not key in key_set:
                    key_set.add(key)
                if num_line_total % print_every == 0:
                    num_key_now = len(key_set)
                    print('Processing line %8d, %8d lines has VT=%s, number of keys now: %8d.' % (num_line_total, num_line_has_qualified_VT, VT, num_key_now))

        num_key = len(key_set)
        print('Totally %10d/%10d lines has VT=%s, total keys number: %10d' % (num_line_has_qualified_VT, num_line_total, VT, num_key))
        ### write to pkl file
        with open(output_pkl, mode='wb') as f:
            pickle.dump(key_set, f)
        print('Finish dumping keys from 1KGP file to ', output_pkl)
    except Exception as e:
        print(e)
    finally:
        pass
    return key_set
        
def read_generator(vcf_gz_file_path, skip_head=False):
    if not os.path.exists(vcf_gz_file_path):
        print('Input file not exists: ', vcf_gz_file_path)
        return
    with gzip.open(vcf_gz_file_path, 'rb') as f:
        for line in f:
            line = line.decode("utf-8")
            if skip_head and line.startswith('#'):
                continue
            yield line

def compare_lines(line_1kgp, line_fmd, VT='SNP', ATGC=set(['A', 'T', 'G', 'C'])):
    line_1kgp_split = line_1kgp.strip().split()
    vt = line_1kgp_split[-1].split('=')[-1]
    if vt != VT:
        return False

    line_fmd_split = line_fmd.strip().split()
    chrom_1kgp, pos_1kgp, ref_1kgp, alt_1kgp = line_1kgp_split[:2] + line_1kgp_split[3:5]

    line_1kgp_split = line_1kgp.strip().split()
    chrom_fmd = line_fmd_split[0][3:]
    pos_fmd, ref_fmd, alt_fmd = [line_fmd_split[1]] + line_fmd_split[3:5]

    ### filter fmd snp
    if len(ref_fmd) != 1 or len(alt_fmd) != 1 or (not ref_fmd in ATGC) or (not alt_fmd in ATGC):
        return False

    return chrom_1kgp != chrom_fmd or pos_1kgp != pos_fmd or ref_1kgp != ref_fmd or alt_1kgp != alt_fmd

def filter_fmd(gen_1kgp, gen_fmd, output_fmd, print_every=1000):
    gen_fmd_generator = read_generator(gen_fmd)
    start = datetime.strftime(datetime.now(), '%Y%m%d-%H%M%S')
    tic = time.time()
    print(start, ', starting process filter_fmd.')
    with gzip.open(output_fmd, 'wb') as writer:
        for i, line_fmd in enumerate(gen_fmd_generator):
            for line_1kgp in gen_1kgp:
                if line_fmd.startswith('#') or compare_lines(line_1kgp, line_fmd):
                    line_fmd_bytes = bytes(line_fmd, 'utf-8')
                    writer.write(line_fmd_bytes)
            if (i+1) % print_every == 0:
                print('Compare fmd line %10d' % i)
    end = datetime.strftime(datetime.now(), '%Y%m%d-%H%M%S')
    toc = time.time()
    time_diff_min = (toc - tic)/ 60
    print('%s, endint process filter_fmd, total time %10.3fmin' % (end, time_diff_min))

def filter_fmd_with_key(key_set, gen_fmd, output_fmd, print_every=1000):
    gen_fmd_generator = read_generator(gen_fmd)
    start = datetime.strftime(datetime.now(), '%Y%m%d-%H%M%S')
    tic = time.time()
    print(start, ', starting process filter_fmd.')

    ATGC=set(['A', 'T', 'G', 'C'])
    with gzip.open(output_fmd, 'wb') as writer:
        for i, line_fmd in enumerate(gen_fmd_generator):
            if not line_fmd.startswith('#'):
                line_fmd_split = line_fmd.strip().split()
                chrom_fmd = line_fmd_split[0][3:]
                pos_fmd, ref_fmd, alt_fmd = [line_fmd_split[1]] + line_fmd_split[3:5]
                ### discard fmd line with vt=snp
                if len(ref_fmd) != 1 or len(alt_fmd) != 1 or (not ref_fmd in ATGC) or (not alt_fmd in ATGC):
                    continue
                key_fmd = (chrom_fmd, pos_fmd, ref_fmd, alt_fmd)
                ### discard fmd line mapped in 1kgp file
                if key_fmd in key_set:
                    continue
            line_fmd_bytes = bytes(line_fmd, 'utf-8')
            writer.write(line_fmd_bytes)
            if (i+1) % print_every == 0:
                print('Compare fmd line %10d' % i)
    end = datetime.strftime(datetime.now(), '%Y%m%d-%H%M%S')
    toc = time.time()
    time_diff_min = (toc - tic)/ 60
    print('%s, end process filter_fmd, total time %10.3fmin' % (end, time_diff_min))

def exec_cmd():
    pass

def exec_cmd_with_key(key_set, input_fmd_args_list):
    with futures.ProcessPoolExecutor(max_workers=20) as executor:
        dict((executor.submit(filter_fmd_with_key, key_set, gen_fmd, output_fmd, 10000), 1) for (gen_fmd, output_fmd) in input_fmd_args_list)

def main(args):
    if not os.path.exists(args.input_1kgp):
        print('Input 1KGP file not exists: ', args.input)
        return

    if not os.path.exists(args.input_fmd_dir):
        print('Input fmd directory not exists: ', args.input_fmd_dir)
        return
    
    if not os.path.exists(args.output_dir):
        os.makedirs(args.output_dir)
        print('Creating directory: ', args.output_dir)

    #check if pkl file exists
    key_set = None
    if not os.path.exists(args.input_pkl):
        key_set = gen_key_stats_from_1kgp(args.input_1kgp, args.input_pkl, print_every=1000)
    else:
        with open(args.input_pkl, mode='rb') as infile:
            key_set = pickle.load(infile)
    
    if key_set is None:
        print('Generate or load key_set failed.')
        return

    input_fmd_suffix = '.vqsr_SNP_INDEL.hc.recaled.vcf.gz'
    output_fmd_suffix = '.vqsr_SNP_INDEL_1KGP.vcf.gz'
    max_num_input_fmd = 22

    input_fmd_filenames = [('FMD%s'+input_fmd_suffix) % n for n in range(max_num_input_fmd)]
    input_fmd_paths = [os.path.join(args.input_fmd_dir, fmd) for fmd in input_fmd_filenames]

    ### save only exists files
    input_fmd_paths = [path for path in input_fmd_paths if os.path.exists(path) and os.path.isfile(path)]
    actual_num_input_fmd = len(input_fmd_paths)

    if actual_num_input_fmd == 0:
        print('No fmd file found in directory: ', args.input_fmd_dir)
        return

    output_fmd_paths = [path.replace(args.input_fmd_dir, args.output_dir).replace(input_fmd_suffix, output_fmd_suffix) for path in input_fmd_paths]

    input_fmd_args_list = [(fmd_i, fmd_o)for fmd_i, fmd_o in zip(input_fmd_paths, output_fmd_paths)]

    exec_cmd_with_key(key_set, input_fmd_args_list)
    print('All done at ', datetime.strftime(datetime.now(), '%Y%m%d-%H%M%S'))

def parse_arguments(argv):
    parser = argparse.ArgumentParser()
    parser.add_argument('--input-1kgp', type=str, 
        default=r'/home/disk/DYG/1000_genomes/ftp/release/20130502/ALL.wgs.phase3_shapeit2_mvncall_integrated_v5b.20130502.sites.vcf.gz',
        help='input 1kgp file')
    parser.add_argument('--input-fmd-dir', type=str, 
        default='/home/disk/yangjing/tianlab_training/DNASeq/ruijin_WGS_pipeline_establishing',
        help='Directory that contains fmd files.')
    parser.add_argument('--input-pkl', type=str, default='./data/1kgp.pkl',
        help='Output pkl file to save keys from 1kgp file.')
    parser.add_argument('--output-dir', type=str, default='./data/fmd_output', 
        help='Directory to save filtered fmd files.')
    parser.add_argument('--verbose', action='store_true', 
        help='print processing info.')
    return parser.parse_args(argv)

if __name__ == '__main__':
    main(parse_arguments(sys.argv[1:]))