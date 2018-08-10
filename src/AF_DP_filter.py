# *********************************#
#          mns 用AD，DP过滤         #
# *********************************#
import vcf
import sys
import argparse # 命令行参数，更好的解析

def main(args):
    writer = open(args.output, mode='w', encoding='utf-8')
    total_line_num, good_line_num = 0, 0
    try:
        with open(args.input, mode='r', encoding='utf-8') as reader:
            for line in reader:
                total_line_num += 1
                # if total_line_num == 500:
                #     break
                if line.strip().startswith('#'):
                    writer.write(line + '\n')
                else:
                    isOk , result = check_line(line)
                    if(isOk):
                        good_line_num += 1
                        writer.write(line + '\n')
                        print('Select no. %dth line with AD=%d, DP=%d, AD/DP=%.3f'%(total_line_num, result[0], result[1], result[2]))
                    else:
                        pass
                        # print('Discard no. %dth line with AD=%d, DP=%d, AD/DP=%.3f'%(total_line_num, result[0], result[1], result[2]))
        if args.verbose:
            print('Found  %d good lines from totally %d lines in file ' %(good_line_num, total_line_num), args.input)

    # except Exception as e:
    #     print(e)
    finally:
        writer.close()

    print('All done, output file is : ', args.output)

def check_line(line, ad_dp_ratio_min=0.01, ad_dp_ratio_max=0.05):
    ad, dp = line.split()[-1].split(':')[1:3]
    ad, dp = int(ad.split(',')[-1]), int(dp)
    if ad == 0 or dp == 0:
        return False, (ad, dp, -1)
    ad_dp = ad / dp * 1.0
    return (ad_dp > ad_dp_ratio_min) and (ad_dp < ad_dp_ratio_max), (ad, dp, ad_dp)

def parse_arguments(argv):
    parser = argparse.ArgumentParser()
    parser.add_argument('input', type=str, help='input vcf file, eg: hello.vcf.')
    parser.add_argument('output', type=str, 
        help='output vcf file, eg: output.vcf. Note, if output file exists, will be replaced.')
    parser.add_argument('--verbose', type=bool, help='print processing info.', default=True)
    return parser.parse_args(argv)

if __name__ == '__main__':
    main(parse_arguments(sys.argv[1:]))