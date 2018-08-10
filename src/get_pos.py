# *********************************#
#          get pos stats           #
# *********************************#
import sys
import argparse # 命令行参数，更好的解析
import os
import gzip
import pickle

def get_pos_stats(input_gz_file, output_txt):
    if not os.path.exists(input_gz_file):
        print('Input 1KGP file not exists: ', input_gz_file)
        return

    writer = open(output_txt, mode='w', encoding='utf-8')

    pos_set = set()
    pos_counter = 0
    try:
        with gzip.open(input_gz_file, 'rb') as f:
            for line in f:
                line = line.decode("utf-8") 
                if line.strip().startswith('#'):
                    continue
                pos = line.split()[1]
                if not pos in pos_set:
                    pos_set.add(pos)
                    pos_counter += 1
                    print('No %8d pos: %s' % (pos_counter, pos))
                    writer.write(pos + ' ')
    except Exception as e:
        print(e)
    finally:
        writer.close()

def get_key_stats(input_gz_file, output_pkl, VT='SNP'):
    if not os.path.exists(input_gz_file):
        print('Input 1KGP file not exists: ', input_gz_file)
        return
    key_set = set()
    try:
        with gzip.open(input_gz_file, 'rb') as f:
            for i, line in enumerate(f):
                line = line.decode("utf-8").strip() 
                if line.startswith('#'):
                    continue
                line_1kgp_split = line.split()
                vt = line_1kgp_split[-1].split('=')[-1]
                if vt != VT:
                    continue
                chrom_1kgp, pos_1kgp, ref_1kgp, alt_1kgp = line_1kgp_split[:2] + line_1kgp_split[3:5]
                key = (chrom_1kgp, pos_1kgp, ref_1kgp, alt_1kgp)
                if not key in key_set:
                    key_set.add(key)
                    print('Processing line %8d' % (i))

        num_key = len(key_set)
        print('Totally %d keys.' % num_key)
        ### write to pkl file
        with open(output_pkl, mode='wb', encoding='utf-8') as f:
            pickle.dump(key_set, f)

    except Exception as e:
        print(e)
    finally:
        pass


def main(args):
    # get_pos_stats(args.input, args.output)
    get_key_stats(args.input, args.output)
    pass



def parse_arguments(argv):
    parser = argparse.ArgumentParser()
    parser.add_argument('--input', type=str, 
        default='/home/disk/DYG/1000_genomes/ftp/release/20130502/ALL.wgs.phase3_shapeit2_mvncall_integrated_v5b.20130502.sites.vcf.gz',
        help='input 1kgp file')
    parser.add_argument('--output', type=str, default='./key_stats.pkl', 
        help='output txt file.')
    parser.add_argument('--verbose', type=bool, help='print processing info.', default=True)
    return parser.parse_args(argv)

if __name__ == '__main__':
    main(parse_arguments(sys.argv[1:]))