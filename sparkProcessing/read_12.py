import boto3
import findspark
findspark.init()
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
#------- setting up the home variable -----------------------------

def parse(f):
    # ###### [AVOID] avoid using the sc = SparkContext() directly. may have muliple sc running error ------
    #sc = SparkContext.getOrCreate()
    #lines = sc.textFile(f)
    # have to filter out some empty list in the original file. don't know why.
    #lines = sc.parallelize(f)
    #remove_empty = lines.flatMap(lambda line: line.split('\n')).filter(lambda x: len(x) > 0)
    #split_data = remove_empty.map(lambda line: line.split(' '))
    # the original parsing 
    #split_data = lines.flatMap(lambda line: line.split('\n')).map(lambda line: line.split(' '))
    #try:
    split_data = f.flatMap(lambda line: line.split('\n')).map(lambda line: line.split(' '))
    #except:
    #    print 'except here. error @: ', f
    #    alert.report(f)
    return split_data

def geo_most_active_user(f,split_data):
    id_count = split_data.map(lambda data: (data[3],1)).reduceByKey(add)
    return id_count

def list_concat(param_list):
    return sc.parallelize(list(itertools.chain.from_iterable(param_list)))


if __name__ == "__main__":
    # ---- start the main program here -----
    s3 = boto3.resource('s3')
    bucket = s3.Bucket('aws21-squeezebox-analysis-tempdata')
    temp = []
    head = 6
    tail = 7

    # ---- collect filename here -----   
    for index in range(head,tail):
        for obj in bucket.objects.filter(Prefix='2017/0'+str(index)):
            key = obj.key
            temp.append(key)
    # ---- parallelize filename here -----
    # print temp
    sc = SparkContext.getOrCreate()
    lines = sc.parallelize(temp)
    date_hour_collection =  lines.map(lambda x:(x[:13],x)).groupByKey().map(lambda x : (x[0], list(x[1]))).sortBy(lambda x:x[0]).collect()
    # ----- use same hour as key, group all the filename according to specific key -------
    # print date_hour_collection
    try:
        for date, filename in date_hour_collection:
            store = []
            unique_id_count = []
            # for a specific date, specific hour period.
            print(date, filename)
            no_slash_date = date.replace('/','_')
            # loop thru all the same prefix filename
            # print [i for i in filename]
            for f in filename:
                # download_obj_filename = f.replace('/','_')
                # print 'Now processing with: ', download_obj_filename
                #f_content = unzip(f,download_obj_filename+'.gz')
                # "s3n://aws21-squeezebox-analysis-tempdata/2017/05/01/00.eu-w1.apps001.log.gz"
                try:
                    f_content = sc.textFile("s3n://aws21-squeezebox-analysis-tempdata/" + f)
                    split_data = parse(f_content)
                    try:
                        per_file_id_count = geo_most_active_user(f,split_data).collect()
                        store.append(per_file_id_count)
                    except Exception as e:
			            alert.report(f)
                except Exception as e:
                    alert.report(f)
                # ---- concatenate the small list into big list, and such big list according to one hour ----
                flat = list_concat(store)
                # ---- reduce by unique id, sum up all the occurence count ------
                id_transaction_pair = flat.reduceByKey(add).sortBy(lambda x: -x[1]).collect()
                var.save('/home/ubuntu/song_user_preference/pickle_output/' + no_slash_date, id_transaction_pair)
                # print_pair(id_transaction_pair)
                # ---- count the # of unique id ------
                # unique_id = flat.distinct().count()
                # unique_id_count.append(unique_id)
                #---- sum up the transaction per person ------
                # total_transaction =
                # var.save(date + '_id_count', unique_id_count)
    except Exception as e:
        #print "Unexpected error:", sys.exc_info()[0]
        logging.error(traceback.format_exc())
	alert.report('crashed.')
        #raise
    print('End of the session.')

