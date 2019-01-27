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
home = expanduser("~")
download_dir = home +'/song_user_preference/'

# ------ setting up the resource for bucket configuration ---------
s3 = boto3.resource('s3')
bucket = s3.Bucket('aws21-squeezebox-analysis-tempdata')
for obj in bucket.objects.filter(Prefix='2017/05/01/00.eu-w1.apps00'):
    key = obj.key

