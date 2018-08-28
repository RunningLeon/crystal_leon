import sys
import os

in_over = sys.argv[1]
in_num_genenum = sys.argv[2]
in_genenum_genename = sys.argv[3]
output = sys.argv[4]

num2genenum_dict = {}
with open(in_num_genenum, 'r') as f2:
    for line2 in f2:
        if line2.startswith('#'):
            continue
        line2 = line2.strip().split()
        num = line2[0]
        genenum = line2[1]
        if not num in num2genenum_dict:
            num2genenum_dict[num] = set()
        num2genenum_dict[num].add(genenum)

genenum2genename_dict = {}
with open(in_genenum_genename, 'r') as f3:
    for line3 in f3:
        if line3.startswith('#'):
            continue
        line3 = line3.strip().split()
        genenum = line3[0]
        genename = line3[1]
        if not genenum in genenum2genename_dict:
            genenum2genename_dict[genenum] = set()
        genenum2genename_dict[genenum].add(genename)

with open(output, mode = 'w') as writer:
    with open(in_over, mode = 'r') as f1:
        for i, line in enumerate(f1, start=1):
            if line.startswith('#'):
                continue
            line = line.strip().split()
            num = line[9]
            chr_, start, end, refalt = line[0:4]
            key = (chr, start, end, refalt)
            if num in num2genenum_dict:
                genenum_set = num2genenum_dict[num]
                for genenum in genenum_set:
                    if genenum in genenum2genename_dict:
                        genename_set = genenum2genename_dict[genenum]
                        for genename in genename_set:
                            line_txt = ' '.join(key) + genename
                            writer.write(line_txt + '\n')
                            if i % 2000 == 0:
                                print('Writing %8d line: %s'%(i, line_txt))
