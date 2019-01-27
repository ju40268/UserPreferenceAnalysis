import boto3
import findspark
findspark.init()
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
#------- setting up the home variable -----------------------------

def unzip(s3_obj_key, download_obj_filename):
    # s3_obj_key: object name store on the s3
    # download_obj_filename: download name store on your local machine
    # ---> should avoid / since it cause directory structure.
    bucket.download_file(s3_obj_key, download_obj_filename)
    f = gzip.open(download_obj_filename,'rb')
    return f
def parse(f):
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

def geo_most_active_user(split_data):
    id_count_pair = split_data.map(lambda data: (data[3],1)).reduceByKey(add).collect()
    return id_count_pair

def list_concat(param_list):
    return sc.parallelize(list(itertools.chain.from_iterable(param_list)))

def remove(filename):
    os.remove(filename)

def print_pair(pair):
    for i, j in pair:
        print (i, j)

if __name__ == "__main__":
    # ---- start the main program here -----
    sc = SparkContext.getOrCreate()
    s3 = boto3.resource('s3')
    bucket = s3.Bucket('aws21-squeezebox-analysis-tempdata')
    temp = []
    # ---- collect filename here -----   
    # for obj in bucket.objects.filter(Prefix='2017/05/02/'):
    #    key = obj.key
    #    temp.append(key)
    # ---- parallelize filename here -----
    date_hour_collection = var.load('/home/ubuntu/song_user_preference/save_pickle/groupby_hour.pickle')
    # sc = SparkContext.getOrCreate()
    # lines = sc.parallelize(pre_filename)
    # date_hour_collection =  lines.map(lambda x:(x[:13],x)).groupByKey().map(lambda x : (x[0], list(x[1]))).sortBy(lambda x:x[0]).collect()
    # ----- use same hour as key, group all the filename according to specific key -------
    unique_id_count = []
    total_transaction_count = []
    for date, filename in date_hour_collection:
        store = []
        # for a specific date, specific hour period.
        print(date)
        no_slash_date = date.replace('/','_')
        # loop thru all the same prefix filename
        # print [i for i in filename]
        for f in filename:
            download_obj_filename = f.replace('/','_')
            print('Now processing with: ', download_obj_filename)
            f_content = unzip(f,download_obj_filename)
            split_data = parse(f_content)
            store.append(geo_most_active_user(split_data))
            remove(download_obj_filename)
        # ---- concatenate the small list into big list, and such big list according to one hour ----
        flat = list_concat(store)
        # ---- reduce by unique id, sum up all the occurence count ------
        id_transaction_rdd = flat.reduceByKey(add).sortBy(lambda x:-x[1])
	# id_transaction_pair = flat.reduceByKey(add).sortBy(lambda x: -x[1])
    id_transaction_pair = id_transaction_rdd.collect()
    var.save('/home/ubuntu/song_user_preference/pickle_output/'+no_slash_date, id_transaction_pair)
	# -----
    total_transaction = id_transaction_rdd.map(lambda x:x[1]).sum()
    total_transaction_count.append((no_slash_date,total_transaction))
    unique_id = flat.distinct().count()
    unique_id_count.append((no_slash_date,unique_id))
    print(unique_id_count)
    print(total_transaction_count)
    var.save('/home/ubuntu/song_user_preference/pickle_output/'+'unique_id',unique_id_count)
    var.save('/home/ubuntu/song_user_preference/pickle_output/'+'total_transaction', total_transaction_count)

# f.close() somewhere 
