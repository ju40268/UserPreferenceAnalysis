from os.path import expanduser
from pyspark import SparkContext, SparkConf
from pyspark import SparkFiles
from operator import add
import itertools
import csv
import os
import numpy as np
#------- setting up the home variable -----------------------------
home = expanduser("~")
download_dir = home +'/song_user_preference/'
download_obj = download_dir + 'temp_data.gz'
file_extension = '.txt'

# ------  write as text file ------------
def write_output(obj, filename):
    with open(filename,'w') as f:
        writer = csv.writer(f)
        writer.writerows(obj)
    f.close()

# ------- how many distint users are there? ----------
def geo_active_user(split_data):
    return split_data.map(lambda data: data[3]).distinct()

def geo_most_active_user(split_data):
    id_count = split_data.map(lambda data: (data[3],1)).reduceByKey(add).collect()
    print '#'*40
    return id_count

def parser(f):
	# ###### [AVOID] avoid using the sc = SparkContext() directly. may have muliple sc running error ------
    sc = SparkContext.getOrCreate()
    #lines = sc.textFile(f)
    # have to filter out some empty list in the original file. don't know why.
    lines = sc.parallelize(f)
    remove_empty = lines.flatMap(lambda line: line.split('\n')).filter(lambda x: len(x) > 0)
    split_data = remove_empty.map(lambda line: line.split(' '))
    # the original parsing 
    #split_data = lines.flatMap(lambda line: line.split('\n')).map(lambda line: line.split(' '))
    return split_data

# ------ setting up the resource for bucket configuration ---------

def upload(filename):
    s3.meta.client.upload_file(full_filename, 'aws21-squeezebox-analysis-output', full_filename)
    
def unzip(key, download_obj):
    bucket.download_file(key, download_obj)
    f = gzip.open(download_obj,'rb')
    return f
    
if __name__ == "__main__":
    # ---- start the main program here -----
    s3 = boto3.resource('s3')
    bucket = s3.Bucket('aws21-squeezebox-analysis-tempdata')
    for obj in bucket.objects.filter(Prefix='2017/05/01/00.eu-w1.apps00'):
        key = obj.key
        print 'Now processing with: ', key
        no_slash_filename = key.replace('/','_')
        f = unzip(key, download_obj)
        split_data = parser(f)
        id_count_pair = geo_most_active_user(split_data)
        full_filename = no_slash_filename[:-7] + file_extension
        write_output(id_count_pair, full_filename)
        upload(full_filename)
        os.remove(full_filename)
        os.remove(download_obj)
    print 'Now end the session.'

# f.close() somewhere 