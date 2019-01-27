import findspark
findspark.init()
print 'find spark session FINISHED.'
import boto3
import gzip
from os.path import expanduser
from pyspark import SparkContext, SparkConf
from pyspark import SparkFiles
from operator import add
import itertools
import csv
import os
import numpy as np

#------- setting up the home variable -----------------------------
'''
home = expanduser("~")
download_dir = home +'/song_user_preference/'
download_obj = download_dir + 'temp_data.gz'
file_extension = '.txt'
def unzip(key, download_obj):
    bucket.download_file(key, download_obj)
    f = gzip.open(download_obj,'rb')
    return f
def parser(f):
    sc = SparkContext.getOrCreate()
    #lines = sc.textFile(f)
    lines = sc.parallelize(f)
    remove_empty = lines.flatMap(lambda line: line.split('\n')).filter(lambda x: len(x) > 0)
    split_data = remove_empty.map(lambda line: line.split(' '))
    #split_data = lines.flatMap(lambda line: line.split('\n')).map(lambda line: line.split(' '))
    return split_data
s3 = boto3.resource('s3')
bucket = s3.Bucket('aws21-squeezebox-analysis-tempdata')
for obj in bucket.objects.filter(Prefix='2017/05/01/00.eu-w1.apps00'):
    key = obj.key
    print 'Now processing with: ', key
    no_slash_filename = key.replace('/','_')
    f = unzip(key, download_obj)
    split_data = parser(f)
'''
sc = SparkContext.getOrCreate()
line = sc.textFile()

