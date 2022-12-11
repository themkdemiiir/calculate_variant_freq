import os
import argparse


parser = argparse.ArgumentParser(
    prog='AF',
    description='Allele Frequency Finder')
parser.add_argument('-vcfdir', '--vcfdirectory')
parser.add_argument('-beddir', '--beddirectory')
args = parser.parse_args()

vcf_directory = args.vcfdirectory
bed_directory = args.beddirectory

#create files locally in given output path. Don't open this to users.
def get_files_dir(dir_path: str, output_file_path: str):
    cmd = f'ls {dir_path} > {output_file_path}'
    os.system(cmd)


def file_yield(file_path):
    for row in open(file_path, 'r'):
        yield row

def process_line(line):
    if not line.startswith('#'):
        return line.split('\t')

#
def path_corrector(directory, outputfile):
    """

    :param directory:
    :param outputfile:
    :return: pathfile in the directory for output file
    """

    if directory.endswith('/'):
        path_file = directory + f'{outputfile}.txt'
    else:
        path_file = directory + f'{outputfile}.txt'
    return path_file


def create_patient_vcf_bed_files_text(vcfdir, beddir):
    """

    :param vcfdir: vcf files directory path
    :param beddir: bed files directory path
    :return: the path to the vcf and bed files names in text
    """
    vcf_text_path = path_corrector(vcfdir, 'vcf')
    bed_text_path = path_corrector(beddir, 'bed')
    get_files_dir(vcfdir, vcf_text_path)
    get_files_dir(beddir, bed_text_path)
    return vcf_text_path, bed_text_path


def get_count_in_bed_files(variant_info, bed_file_directory, bed_text_path):
    """

    :param variant_info: variant information in list
    :param bed_file_directory: bed files directory path
    :param bed_text_path: all bed files names in text file
    :return: total count in integer
    """
    covered_count = 0
    for f in file_yield(bed_text_path):
        bed_file = bed_file_directory + f.strip('\n')
        if bed_file.endswith('.bed'):
            for l in file_yield(bed_file):
                l = l.strip('\n').split('\t')
                l[0] = str(l[0]).strip('chr')
                if str(variant_info[0]) == l[0] and int(variant_info[1]) in range(int(float(l[1]) + 1),
                                                                             int(float(l[2]) + 1)):
                    covered_count += 1

    return covered_count


def get_het_hom_counts_in_vcf_files(vcf_text_path, vcf_file_directory, comparing_variant, self_file):

    """

    :param vcf_text_path:
    :param vcf_file_directory:
    :param comparing_variant:
    :param self_file:
    :return: het and hom count in integer
    """

    het = 0
    hom = 0
#too much if conditions looks bad will look later
    for f1 in file_yield(vcf_text_path):
            vcf_file = vcf_file_directory + f1.strip('\n')
            if vcf_file.endswith('.vcf'):
                for l1 in file_yield(vcf_file):
                    variant_compared = process_line(l1)
                    if variant_compared:
                        necc_compared_variant_columns = [variant_compared[x].strip('\n') for x in [0, 1, 3, 4, 9]]
                        if ''.join(necc_compared_variant_columns[0:3])==''.join(comparing_variant[0:3]):
                            print(necc_compared_variant_columns, comparing_variant)
                            if necc_compared_variant_columns[4] == '1/1':
                                hom += 1
                            else:
                                het += 1
    return het, hom


def make_variant_frequency_csv(vcf_file_directory, vcf_text_path, bed_text_path, bed_file_directory):
    """

    :param vcf_file_directory: vcf files directory path
    :param vcf_text_path: all vcf files names in text file
    :param bed_text_path: all bed files names in text file
    :param bed_file_directory: bed files directory path
    :return: csv file
    """
    file_path = f'{vcf_file_directory}/variant_freq.csv'
    with open(file_path, 'w') as variant_file:
      for f3 in file_yield(vcf_text_path):
    
        vcf_file = vcf_file_directory + f3.strip('\n')
        for i in file_yield(vcf_file):
            
            variant = process_line(i)
            if variant is not None and len(variant) == 10:
              
                necc_variant_columns = [variant[x].strip('\n') for x in [0, 1, 3, 4, 9]]
                total_count = get_count_in_bed_files(necc_variant_columns, bed_file_directory, bed_text_path)
                het_count, hom_count = get_het_hom_counts_in_vcf_files(vcf_text_path, vcf_file_directory, necc_variant_columns, f3)
                calculated_variant_freq = (2*hom_count+het_count)/(2*total_count)
                variant_file.write(f"{','.join(necc_variant_columns[:4])},{het_count},{hom_count},{total_count},{calculated_variant_freq}\n")
    return file_path
vcf_text, bed_text = create_patient_vcf_bed_files_text(vcf_directory, bed_directory)
make_variant_frequency_csv(vcf_directory, vcf_text, bed_text, bed_directory)