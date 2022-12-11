import hashlib
from typing import List, Any

import psycopg2
from psycopg2 import Error
import os
import argparse
import Database


def file_yield(file_path):
    for row in open(file_path, 'r'):
        yield row

def hash_seq(s):
    result = hashlib.md5(s.encode())
    return result.hexdigest()

def check_table_exist(table_name):
    return table_name in db.currently_existing_tables

def save_unique_rows(path_to_md5sum,output):
      df = pd.read_csv(path_to_md5sum)
      df1 = df.drop_duplicates(keep='first')
      df1.to_csv(output, index=False,header=False)

class FrequencyTable:

    def __init__(self, frequency_csv_path):
        self.frequency_csv_path = frequency_csv_path
        self.sorted_unique_path = None
        self.make_hash_ids()

    def make_hash_ids(self):
        hash_file_path = '/'.join(self.frequency_csv_path.split('/')[:-1])+ '/md5sum_variant_frequency.csv'
        with open(hash_file_path, w) as hash_file:
            for line in file_yield(self.frequency_csv_path):
                unique_variant_hash =  hash_seq('-'.join(line.split('\t')[:3]))
                hash_id_line = f"{unique_variant_hash},{line}"
                hash_file.write(hash_id_line)
        sorted_unique_file_path = '/'.join(self.frequency_csv_path.split('/')[:-1])+ '/unique_variant_frequency.csv'
        save_unique_rows(hash_file_path, sorted_unique_file_path)
        self.sorted_unique_path = sorted_unique_file_path

    def table_frequency_create(self ,database):
        create_frequency_table = f'''CREATE TABLE variant_frequency_table (
                                        md5sum VARCHAR(128) NOT NULL UNIQUE ,
                                        chr int not null,
                                        pos int not null
                                        ref_N char not null
                                        aa_N char not null
                                        het_count int not null
                                        hom_count int not null
                                        total_count int not null
                                        frequency float not null;'''
        database.query(create_frequency_table)
        dump_data_to_temp = rf'''COPY variant_frequency_table FROM '{self.sorted_unique_path}' USING DELIMITERS E',' WITH NULL AS '\null' CSV header QUOTE E'\b' ESCAPE '\';'''
        database.query(dump_data_to_temp)
        return check_table_exist('variant_frequency_table')

    def update_frequency_table(self, database):
        create_temp_table = '''CREATE TABLE temp (
                                        md5sum VARCHAR(128) NOT NULL UNIQUE,
                                        chr int not null,
                                        pos int not null
                                        ref_N char not null
                                        aa_N char not null
                                        het_count int not null
                                        hom_count int not null
                                        total_count int not null
                                        frequency float not null;'''
        database.query(create_temp_table)
        dump_md5 = rf'''COPY temp FROM '{self.sorted_unique_path}' USING DELIMITERS E',' WITH NULL AS '\null';'''
        database.query(dump_md5)
        insert_query = '''insert into varint_frequency_table select distinct * from temp on conflict (md5sum) do nothing;'''
        database.query(insert_query)
        drop_table = '''DROP TABLE temp;'''
        database.query(drop_table)

parser = argparse.ArgumentParser(description='Add table file to the postgre-sql database')

parser.add_argument('--username', '-u', type=str, help='postgresql username', required=True)
parser.add_argument('--password', '-p', type=str, help='postgresql password', required=True)
parser.add_argument('--dbname', '-db', type=str, help='name of the database to be altered', required=True)
parser.add_argument('--filepath', '-fp', type=str,
                    help='full path of the expected csv file with sequences',
                    required=True)
parser.add_argument('--header', '-he', type=str,
                    help='if header exists in the first line input file, add -he to the command', action='store_true')
parser.add_argument('--operation', '-op', type=str,
                    help='Type u if you want to update an existing table in the database (on conflict -md5sum- do nothing), a if you want to add a new table.',
                    required=True)
parser.add_argument('--tablename', '-n', type=str, help='exact name of the table you want to add/update',
                    required=True)

args = parser.parse_args()


database = Database(username=args.username, password=args.password, database=args.dbname)

md5sum_file_path = FrequencyTable(args.filepath).make_hash_ids()

if args.opearion == 'a':
    FrequencyTable(args.filepath).table_frequency_create(db)
elif args.opearion == 'u':
    FrequencyTable(args.filepath).update_frequency_table(db)
