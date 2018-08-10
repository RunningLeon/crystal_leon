import sys
import os

INPUT_vcf = sys.argv[1]

with open(INPUT_vcf,'r') as f:
	
	ratio_str_dict = {}
	count_homo = 0
	count_het = 0
	for line in f:		
		if line.startswith('#'):
			continue
		line = line.strip().split()
		chrx = line[0]
		if chrx != "chrX":
			continue

		ratio_str = line[9].split(':')[0] # 0/1 1/1 2/2 etc
		#print(ratio_str)
		ratio_str_left = ratio_str.split('/')[0]
		ratio_str_right = ratio_str.split('/')[1]
		if not ratio_str in ratio_str_dict:
			ratio_str_dict[ratio_str] = 1
		else:
			ratio_str_dict[ratio_str] += 1
		if ratio_str_left == ratio_str_right:
			count_homo += 1
		else:
			count_het += 1
	for ratio_str,ratio_str_dict[ratio_str] in ratio_str_dict.items():
		print(ratio_str + ' ' + str(ratio_str_dict[ratio_str]) + '\n')

	
	score = 0
	if count_het == 0:
	    print('Warning: count_het is zero.')
	else:
	    score = count_homo / float(count_het)

	if score > 4:
		print(sys.argv[1] + ' is a boy')
	else:
		print(sys.argv[1] + ' is a girl')
		
		
		

