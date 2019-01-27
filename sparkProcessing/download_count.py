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
import alert
#------- setting up the home variable -----------------------------

def unzip(s3_obj_key, download_obj_filename):
    bucket.download_file(s3_obj_key, download_obj_filename)
    f = gzip.open(download_obj_filename,'rb')
    return f
def parse(f):
    sc = SparkContext.getOrCreate()
    lines = sc.parallelize(f) 
    remove_empty = lines.flatMap(lambda line: line.split('\n')).filter(lambda x: len(x) > 0)
    split_data = remove_empty.map(lambda line: line.split(' '))
    return split_data
         
def geo_most_active_user(split_data):
    id_count_pair = split_data.map(lambda data: (data[3],1)).reduceByKey(add).collect()
    return id_count_pair
      
def partitionIp2city(iter):
    from geoip2 import database
    def ip2city(ip):
        try:
            city = reader.city(ip).country.name
            if city == 'United States': city = 'United States of America'
        except:
            city = 'not found'
        return city
    reader = database.Reader(SparkFiles.get(geoDBpath))
    return [ip2city(ip) for ip in iter]

def transaction_per_country(split_data):
    city_mapping = split_data.map(lambda data: data[4]).mapPartitions(partitionIp2city)
    city_count = city_mapping.map(lambda x:(x,1)).reduceByKey(add)
    sorted_city_count = city_count.sortBy(lambda x: -x[1]).collect()
    return sorted_city_count

def list_concat(param_list):
    return sc.parallelize(list(itertools.chain.from_iterable(param_list)))

def remove(filename):
    os.remove(filename)


if __name__ == "__main__":
    # ---- start the main program here -----
    sc = SparkContext.getOrCreate()
    s3 = boto3.resource('s3')
    bucket = s3.Bucket('aws21-squeezebox-analysis-tempdata')
    date_hour_collection = var.load('/home/ubuntu/song_user_preference/groupby.pickle')
    # ----- use same hour as key, group all the filename according to specific key -------
    geoDBpath = 'GeoIP2-City.mmdb'
    sc.addFile(geoDBpath)

    for date, filename in date_hour_collection:
        store = []
        no_slash_date = date.replace('/','_')
        for f in filename:
            download_obj_filename = f.replace('/','_')
            print('Now processing with: ', download_obj_filename)
            f_content = unzip(f,download_obj_filename)
            split_data = parse(f_content)
            store.append(transaction_per_country(split_data))
            remove(download_obj_filename)
        flat = list_concat(store)
        country_count_pair = flat.reduceByKey(add).sortBy(lambda x:-x[1]).collect()
        #  = id_transaction_rdd.collect()
        var.save('/home/ubuntu/song_user_preference/pickle_output/'+no_slash_date, country_count_pair)
    alert.report('end of the program.')