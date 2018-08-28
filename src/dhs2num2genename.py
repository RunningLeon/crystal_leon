import sys
import os

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

in_over = sys.argv[1]
in_num_genenum = sys.argv[2]
in_genenum_genename = sys.argv[3]
output = sys.argv[3]
over_dict = {}
num_set = set()
genename_set = set()
with open(in_over, mode = 'r') as f1:
    for line in f1:
        line = line.strip().split()
        num = line[9]
        chr, start, end, refalt = line[0:4]
        key = (chr, start, end, refalt)
        over_dict[key].append(num)
        print(over_dict)
    with open(in_num_genenum, mode = 'r') as f2:
        for line2 in f2:
            line2 = line2.strip().split()
            num_copy = line2[0]
            genenum = line2[1]
            if over_dict[key] == num_copy:
                num_set.add(genenum)
        with open(in_genenum_genename, mode = 'r') as f3:
            for line3 in f3:
                line3 = line3.strip().split()
                genenum_copy = line3[0]
                genename = line3[1]
                for i in num_set:
                    if i == genenum_copy:
                        num_set.add(genename)
            with open (output, mode = 'w') as writer:
                writer.write(over_dict + ' ' + num_set + '\n')


