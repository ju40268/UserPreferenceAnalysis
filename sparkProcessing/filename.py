import boto3
import findspark
findspark.init()
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
def save_filename(file_list):
    with open('filename_collection.pickle', 'w') as f:  # Python 3: open(..., 'wb')
        pickle.dump(file_list, f)
    
if __name__ == "__main__":
    # ---- start the main program here -----
    s3 = boto3.resource('s3')
    bucket = s3.Bucket('aws21-squeezebox-analysis-tempdata')
    filename_list = []
    for obj in bucket.objects.all():
        filename_list.append(obj.key)
    save_filename(filename_list)
