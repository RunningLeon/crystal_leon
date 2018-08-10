import sys
import os
import argparse
import time

def gene_count(input_vcf, output_csv, print_every=10):
        header = 'gene number, count\n'
        with open(output_csv, mode='w') as writer:

                writer.write(header)
                count_dict = {}
                with open(input_vcf, mode='r') as reader:
                        for i, line in enumerate(reader):#在所有的行数的范围内
                                line = line.strip()
                                if line.startswith('#'):
                                        continue
                                gene_number = line.split()[3].split(':')[1]
                                if not gene_number in count_dict:
                                        count_dict[gene_number] = 0
                                else:
                                        count_dict[gene_number] += 1
                                if (i + 1) % print_every == 0:
                                        print('Processing line %8d ' % (i))

                for gene_number, count in count_dict.items():
                        writer.write(gene_number + ', %s\n'%count)

def main(args):
        if not os.path.exists(args.input_fmd_dir):
                print('Input fmd dir not exists: ',args.input_fmd_dir)
        input_vcf = args.input_fmd_dir + '/FMD' + args.fmd_number + '_gene.exonic_variant_function'
        if not os.path.exists(args.output_csv_dir):
                os.makedies(args.output_csv_dir)
        output_csv = args.output_csv_dir + '/FMD' + args.fmd_number + '.gene_count.csv'
        gene_count(input_vcf,output_csv)
        print('All done')

def parse_arguments(argv):
        parser = argparse.ArgumentParser(description='Process some integers.')
        parser.add_argument('fmd_number',type=str,
                help='fmd number')
        parser.add_argument('--input-fmd-dir',type=str,
                help='fmd directory', 
                default='/home/disk/yangjing/tianlab_training/DNASeq/ruijin_WGS_pipeline_establishing/FMD_depth_filter_after_VQSR_1KGP/fmd_snv_distance_filt_out/annovar_FMD/test')
        parser.add_argument('--output-csv-dir',type=str,
                help='output csv after count gene_number', 
                default='/home/disk/yangjing/tianlab_training/DNASeq/ruijin_WGS_pipeline_establishing/FMD_depth_filter_after_VQSR_1KGP/fmd_snv_distance_filt_out/annovar_FMD/test')
        return parser.parse_args(argv)

if __name__ == '__main__':
        main(parse_arguments(sys.argv[1:]))                          