import boto3
import findspark
# ----- avoid the env var not set ------------
findspark.init()
print 'find spark session FINISHED.'
# --------------------------------------------
import traceback
import logging
import gzip
from os.path import expanduser
from pyspark import SparkContext, SparkConf
from pyspark import SparkFiles
from operator import add
import itertools
import csv
import os
import numpy as np
import pickle
import var
import pandas as pd
import sys

def download(s3_obj_key, download_obj_filename):
    bucket.download_file(s3_obj_key, download_obj_filename)
    with open(download_obj_filename) as f:
        f_content = pickle.load(f)
    return  f_content

# with open("records.tsv", "w") as record_file:
#     for record in SeqIO.parse("/home/fil/Desktop/420_2_03_074.fastq", "fastq"):
#         record_file.write("%s %s %s\n" % (record.id,record.seq, record.format("qual")))

if __name__ == "__main__":
    s3 = boto3.resource('s3')
    bucket = s3.Bucket('aws21-squeezebox-analysis-output')   
    sc = SparkContext.getOrCreate()
    dump_file = []
    month = str(sys.argv[1])
    for obj in bucket.objects.filter(Prefix=month):
        try:
            print 'Now processing with bucket: ', obj.key
            pickle_f = download(obj.key, obj.key)
            f = sc.parallelize(pickle_f)
            dump_file.append([obj.key[8:10], obj.key[11:13], f.map(lambda x : x[1]).sum()])            
            os.remove(obj.key)
        except Exception as e:
            print e
    
    var.save('date_transaction_list_' + month, dump_file)
    date_transaction_list = var.load('date_transaction_list_' + month + '.pickle')
    df = pd.DataFrame(date_transaction_list)
    df.to_csv(month + '.csv', index=False, header=['day','hour','value'])
    # csv.writer(file('output.tsv', 'w+'), delimiter='\t').writerows(csv.reader(open("save_csv.csv")))
    print 'End'
