#!/usr/bin/python3
# -*- coding: utf-8 -*-
# *************************************************************************************#
#               same_snp_pool from all the vcf files we have                           #
# Usage: python3 pickl_fake_snp.py sample.vcf filename_Already_run filename_snp_stat   #
# *************************************************************************************#

import sys
import os
import argparse
import pickle
import time
from datetime import datetime

def read_generator(vcf_file_path, skip_head=False):
	"""
	读取文件， 返回文件内容的生成器。 注意：一个生成器只能用for循环遍历一次
	"""
	if not os.path.exists(vcf_file_path):
		print('Input file not exists: ', vcf_file_path)
		return
	with open(vcf_file_path, 'r') as f:
		for line in f:
			if skip_head and line.startswith('#'):
				continue
			yield line

def write_csv(input_csv, data_list):
	"""
		输入csv文件和数据， 将数据写入csv文件。
	"""
	### 如果存在该文件， 直接覆盖原文件
	csv_dir = os.path.split(input_csv)[0] ##获得csv文件的上一级文件夹， 如果该文件夹不存在， 则创建
	if csv_dir != '' and not os.path.exists(csv_dir):
		os.makedirs(csv_dir) ### 创建文件夹
		print('Creating directory: ', csv_dir)

	with open(input_csv, 'w') as f:
		for line in data_list:
			f.write(line + '\n')
	print('Finish writing file :', input_csv)

def update_data(input_vcf, line_info_dict, vcf_files_dict, print_every=2000):
	"""
		主要功能： 输入vcf文件， 更新记录key出现次数的字典line_info_dict和 记录已经运行的vcf文件的集合vcf_files_dict
	"""
	start_time = datetime.strftime(datetime.now(), '%Y%m%d-%H%M%S')

	line_key_check_set = set() ### 为了判断同样的key在一个文件中出现多次时， line_info_dict 只加1, 需创建这个集合
	update_counter = 0 ### 记录本文件对line_info_dict更新次数
	### 1. 检测文件是否存在
	if not os.path.exists(input_vcf):
		print('File not exists: ', input_vcf)
		return False
	### 2. 检测文件是否已经运行过
	if input_vcf in vcf_files_dict:
		print('File already ran :', input_vcf)
		return False
	else:
	### 3. 更新vcf_files_dict
		vcf_files_dict[input_vcf] = start_time ### key为vcf文件绝对路径, 值为文件执行日期

	print(start_time, ', starting process file: ', os.path.split(input_vcf)[-1])
	tic = time.time()
	### 4. 读取文件
	file_generator = read_generator(input_vcf, skip_head=True)
	for i, line in enumerate(file_generator):
		line_split = line.strip().split()
		if len(line_split) < 5:
			print('Split %4d line wrong'%(i+1))
			continue
		chr_, pos, ref, alt = line_split[:2] + line_split[3:5]
		key_tuple = (chr_, pos, ref, alt)
		### 5. 更新line_info_dict
		### 判断key是否在该文件中第一次出现, 也就是当key第一次出现的时候， 才会去对line_info_dict进行更新
		if not key_tuple in line_key_check_set:
			line_key_check_set.add(key_tuple)
			update_counter += 1
			if key_tuple not in line_info_dict: # 不存在
				line_info_dict[key_tuple] = 1
			else: # 存在
				line_info_dict[key_tuple] += 1
		### print info
		if (i+1) % print_every == 0:
			print('Processing %8d line, update %8d counter to line_info_dict.'%((i+1), update_counter))

	end_time = datetime.strftime(datetime.now(), '%Y%m%d-%H%M%S')
	toc = time.time()
	time_diff_min = (toc - tic)/ 60
	print('%s, end, total time %10.3fmin' % (end_time, time_diff_min))
	if update_counter == 0:
		return False
	return True

def get_all_files(input_path_list, extension='.vcf'):
		vcf_file_paths = []
		for path in input_path_list:
				if os.path.isfile(path) and path.endswith(extension):
					vcf_file_paths.append(path)
				elif os.path.isdir(path):
					vcf_file_paths += [os.path.join(path, p) for p in os.listdir(path) if p.endswith(extension)]
				else:
					pass
		print('Totall %5d vcf files found.'%(len(vcf_file_paths)))
		return vcf_file_paths


def main(args):
	time_st = time.time()
	### 1. 检测保存数据的pkl文件是否存在, 存在, load数据, 不存在, 就新建空变量
	need_to_update = False ### 判断是否需要更新pkl文件
	line_info_dict, vcf_files_dict = {}, {} ##不存在， 新建空变量
	if os.path.exists(args.pkl_file): ### 存在， load
		print('Loading data from file : ', args.pkl_file)
		with open(args.pkl_file, mode='rb') as f:
			line_info_dict, vcf_files_dict = pickle.load(f) ### 存在， 加载数据
	### 2. 送如vcf 文件， 更新line_info_dict 和  vcf_files_dict
	input_vcf_list = get_all_files(args.input_vcf_list)
	if len(input_vcf_list) == 0:
			print('No vcf file was found！')
			return
	for vcf_file in input_vcf_list:
		vcf_abs_path = os.path.abspath(vcf_file) ###获得vcf文件绝对路径
		success = update_data(vcf_abs_path, line_info_dict, vcf_files_dict, print_every=5000)
		if success: ## 如果有一个vcf文件成功运行， 则line_info_dict, vcf_files_dict 需要更新
			need_to_update = True
	
	### 3. 输出csv文件
	vcf_file_info_list = sorted(vcf_files_dict.items(), key=lambda x: x[1]) ### 根据执行日期对dict排序, 得到元素为(filename, date)的list
	data_list = [pair[1] + ', ' + pair[0] for pair in vcf_file_info_list]
	write_csv(args.output_filename_csv, data_list) ### 输出所有已经处理的cvf文件路径

	line_info_list = sorted(line_info_dict.items(), key=lambda x: x[1], reverse=True) ### 根据计数次数对dict进行从大到小排序， 得到的是以（key, 次数)为元素的list
	data_list = [','.join(pair[0]) + ',' + str(pair[1]) for pair in line_info_list] ## 注意是line_info_list，不是line_info_dict
	write_csv(args.output_stats_csv, data_list) ###输出统计csv文件

	#### 4. 序列化line_info_dict, vcf_files_dict 到本地， 直接覆盖原文件
	pkl_dir = os.path.split(args.pkl_file)[0]   ###文件夹不存在，则直接创建
	if pkl_dir != '' and not os.path.exists(pkl_dir):
		os.makedirs(pkl_dir)
		print('Creating directory: ', pkl_dir)
		
	if need_to_update:
		with open(args.pkl_file, 'wb') as f:
			data = (line_info_dict, vcf_files_dict) ## 合并成二元组对象
			pickle.dump(data, f)
			print('Updated ', args.pkl_file)

	time_total = (time.time()-time_st)
	date_now = datetime.strftime(datetime.now(), '%Y%m%d-%H%M%S')
	print('All done at %s, totally time consuming: %10.3f seconds'%(date_now, time_total))

def parse_arguments(argv):
	parser = argparse.ArgumentParser()
	parser.add_argument('input_vcf_list', nargs='+', type=str,
		default='.',
		help='You should input one or many vcf file or directory that needs to be processed. ')
	parser.add_argument('-n', '--output-filename-csv', type=str, 
		default='./data/ran_vcf_filename.csv',
		help='Give a filename to a csv file that save vcf files you have already run, it will be a *.csv file.')
	parser.add_argument('-o', '--output-stats-csv', type=str, 
		default='./data/ran_data_stats.csv',
		help='Give a filename to save same_SNP_stat pool you have already run before, it will be a *.csv file.')
	parser.add_argument('-p', '--pkl-file', type=str, 
		default='./data/data.pkl',
		help='Pickle file that saves data of dict and set.')
	return parser.parse_args(argv)

if __name__ == '__main__':
	main(parse_arguments(sys.argv[1:]))


