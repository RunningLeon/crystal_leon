# 读入打开文件input文件
import sys
import os
import gzip
import argparse
import time
# 按行读

def filt_snv_distance(input_vcf, output_vcf):
# 每一行按空格分开
# 如果第一列相同
# 取第二列相减，下一行减去上一行
# 如果相减的值≥5则写入output文件
# 关闭文件
  with gzip.open(output_vcf, mode='wb') as writer:
      with gzip.open(input_vcf, mode='rb') as reader:
        lines_bytes = reader.readlines()
        for i in range(1, len(line_bytes)):
          line_before_bytes = lines_bytes[i-1]
          line_now_bytes = lines_bytes[i]
          line_before_str, line_now_str = [x.decode("utf-8").strip() for x in [line_before_bytes, line_now_bytes]]
          if line_before_str.startswith('#'):
            continue
          snv_distance = line_now_str.split('')[1] - line_before_str.split('')[1]
          if snv_distance >= 5:
            writer.write(line_now_bytes)
def main(args):
  # 打开文件，写入#开头的行
  input_vcf = args.input_fmd_dir + '/FMD' + args.fmd_number + 'filter_vqsr_1kgp_depth.vcf.gz'
  output_vcf = args.output_fmd_dir + '/' +  args.fmd_number + '.filter_vqsr_1kgp_depth_snv.vcf.gz'
  filt_snv_distance(input_vcf, output_vcf)

def parser_arguments(argv):
  parser = argparse.ArgumentParser()
  parser.add_argument('fmd_number', type=str, 
    help='fmd number')
  parser.add_argument('--input-fmd-dir', type=str, 
    default='/home/disk/yangjing/tianlab_training/DNASeq/ruijin_WGS_pipeline_establishing/FMD_depth_filter_after_VQSR_1KGP', 
    help='Directory that contains fmd files')
  parser.add_argument('--output-fmd-dir', type=str, 
    default='/home/disk/yangjing/tianlab_training/DNASeq/ruijin_WGS_pipeline_establishing/FMD_depth_filter_after_VQSR_1KGP/fmd_snv_distance_filt_out', 
    help='Output directory of fmd files after snv_distance filter')
  return parser.parse_args(argv)


if __name__ == '__main__':
    main(parse_arguments(sys.argv[1:]))