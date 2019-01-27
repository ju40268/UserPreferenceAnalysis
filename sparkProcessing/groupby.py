import boto3
import findspark
findspark.init()
print 'find spark session FINISHED.'
import pickle
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
def load_filename(obj='/home/ubuntu/song_user_preference/save_pickle/filename_collection.pickle'):
    with open(obj) as f:  # Python 3: open(..., 'rb')
        filename = pickle.load(f)
    return filename
def save_groupby(group_list):
    with open('groupby_hour.pickle', 'w') as f:  # Python 3: open(..., 'wb')
        pickle.dump(group_list, f)
if __name__ == "__main__":
    # ---- start the main program here -----
    # s3 = boto3.resource('s3')
    # bucket = s3.Bucket('aws21-squeezebox-analysis-tempdata')
    sc = SparkContext.getOrCreate()
    lines = sc.parallelize(load_filename())   
    date_hour_collection =  lines.map(lambda x:(x[:13],x)).groupByKey().map(lambda x : (x[0], list(x[1]))).collect()
    save_groupby(date_hour_collection)
    print 'Session End Successfully.'
	
