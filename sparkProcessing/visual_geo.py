import sys
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

def download(s3_obj_key, download_obj_filename):
    bucket.download_file(s3_obj_key, download_obj_filename)
    with open(download_obj_filename) as f:
        f_content = pickle.load(f)
    return  f_content

def list_concat(param_list):
    return sc.parallelize(list(itertools.chain.from_iterable(param_list)))

if __name__ == "__main__":
    s3 = boto3.resource('s3')
    bucket = s3.Bucket('aws21-squeezebox-analysis-output-geo')   
    sc = SparkContext.getOrCreate()
    date_temp = []
    list_container = []
    dump_file = []
    month = str(sys.argv[1])
    # for obj in bucket.objects.filter(Prefix = month):
        # try:
            # print obj.key[:10]
            # groupByKey().map(lambda x : (x[0], list(x[1])))
            # date_key.append(obj.key[:10])
            # print 'Now processing with bucket: ', obj.key
            # pickle_f = download(obj.key, obj.key)
            # f = sc.parallelize(pickle_f)
            # day = obj.key[8:10]
            # hour = obj.key[11:13]

            # dump_file.append([obj.key[8:10], obj.key[11:13], f.distinct().count()])  
            # print obj.key[8:10]
            # print obj.key[11:13]
            # print f.collect()
            # list_container.append()
            # os.remove(obj.key)
        # except Exception as e:
        #     print e
    for obj in bucket.objects.filter(Prefix = month):
        date_temp.append(obj.key)
    # print date_temp
    lines = sc.parallelize(date_temp)
    date_hour_collection =  lines.map(lambda x:(x[:10],x)).groupByKey().map(lambda x : (x[0], list(x[1]))).sortBy(lambda x:x[0]).collect()
    date_country_count_collection = []
    # # print date_hour_collection
    for date, hour in date_hour_collection:
        everymonth_day = date[8:10]
        print 'Now processing with:', everymonth_day
        for h in hour:
            # singleday_hour =  h[-9:-7]
            pickle_f = download(h, h)
            f = sc.parallelize(pickle_f).collect()
            list_container.append(f)
            os.remove(h)
        rdd_list = list_concat(list_container)
        # country_count_collection = rdd_list.groupByKey().map(lambda x : (x[0], list(x[1])))
        output = rdd_list.reduceByKey(add).sortBy(lambda x: -x[1]).collect()
        # date_country_count_collection.append([month, everymonth_day, output])

    #var.save('geo_date_transaction_list_' + month, date_country_count_collection)
    #date_transaction_list = var.load('geo_date_transaction_list_' + month + '.pickle')
    #df = pd.DataFrame(date_transaction_list)
    #df.to_csv( 'geo_' + month + '.csv', index=False, header=['month','day','list'])
    df = pd.DataFrame(output)
    df.to_csv( 'geo_only_list_' + month + '.csv', index=False, encoding='utf-8',header=['Country', 'transaction'])
    print 'End'
