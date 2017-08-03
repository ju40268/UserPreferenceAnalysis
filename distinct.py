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
import alert
import json

def download(s3_obj_key, download_obj_filename):
    bucket.download_file(s3_obj_key, download_obj_filename)
    with open(download_obj_filename) as f:
        f_content = pickle.load(f)
    return  f_content

if __name__ == "__main__":
    s3 = boto3.resource('s3')
    bucket = s3.Bucket('aws21-squeezebox-analysis-output')   
    sc = SparkContext.getOrCreate()
    dump_file = []
    for obj in bucket.objects.filter(Prefix='2016_01'):
    	try:
	        print 'Now processing with bucket: ', obj.key
	        pickle_f = download(obj.key, obj.key)
	        f = sc.parallelize(pickle_f)

	        # dump all the file into json format       
	        #data = {
	        #   'total_transaction' : f.map(lambda x : x[1]).sum(),
	        #   'distinct_userid_count' : len(pickle_f),
	        #   'most_active_user' : f.take(50),
	        #}
	        # using date as dic key
	        #json_container[obj.key[:13]] = data
	        # remove the download file
	        os.remove(obj.key)
	    except Exception as e:
	    	alert.report(e)


    