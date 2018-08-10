import vcf

INPUT_FILE = r'C:/Users/yangjing/Desktop/output.vcf/output.vcf'
OUTPUT_FILE = r'C:/Users/yangjing/Desktop/output.vcf/output_modified.vcf'


global_counter=0

def check_record(rec, AF_MIN=0.01, DP_MIN=500, verbose=False):
    af = 0
    dp = 0
    global global_counter
    if hasattr(rec, 'samples'):
        try:

            af = rec.samples[0]['AF']
            if isinstance(af, list) and af:
                af = af[0]
            if af < AF_MIN:
                return False
        except Exception as e:
            raise ValueError('Wrong when get AF value')

    if hasattr(rec, 'INFO'):
        try:
            dp = rec.INFO['DP']
            if dp <= DP_MIN:
                return False
        except Exception as e:
            raise ValueError('Wrong when get DP value')
    
    global_counter += 1

    if verbose:
        print('Select no. %d record with AF=%.3f, DP=%d'%(global_counter, af, dp))        
    return True

def process_vcf(r_file=INPUT_FILE, w_file=OUTPUT_FILE, verbose=False):
    vcf_reader = vcf.Reader(open(r_file, 'r'))
    vcf_writer = vcf.Writer(open(w_file, 'w'), vcf_reader)

    for rec in vcf_reader:
        if check_record(rec, verbose=verbose):
            vcf_writer.write_record(rec)



def main():
    process_vcf(verbose=True)

if __name__ == '__main__':
    main()