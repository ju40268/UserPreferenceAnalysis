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
import geoip2.database

geoDBpath = 'GeoLite2-City.mmdb'
sc.addFile(geoDBpath)

#------- setting up the home variable -----------------------------

def parse(f):
    split_data = f.flatMap(lambda line: line.split('\n')).map(lambda line: line.split(' '))
    return split_data

def geo_most_active_user(f,split_data):
    try:
        id_count_pair = split_data.map(lambda data: (data[3],1)).reduceByKey(add).collect()
    except IndexError:
        print('Index Error @ filename: ', f)
        alert.report(f)
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
if __name__ == "__main__":
    # ---- start the main program here -----
    s3 = boto3.resource('s3')
    bucket = s3.Bucket('aws21-squeezebox-analysis-tempdata')
    all_filename = []
    # ---- collect filename here -----   
    date_hour_collection = var.load('groupby')
    for date, filename in date_hour_collection:
        no_slash_date = date.replace('/','_')
        for f in filename:
            try:
                f_content = sc.textFile("s3n://aws21-squeezebox-analysis-tempdata/" + f)
                split_data = parse(f_content)
                per_file_country_count = transaction_per_country(split_data)
                store.append(per_file_country_count)
            except Exception as e:
                alert.report('Crashed.')
        flat = list_concat(store)
        country_count_pair = flat.reduceByKey(add).sortBy(lambda x: -x[1]).collect()
        var.save(no_slash_date[13:], country_count_pair)

    alert.report('Successfully end the program.')
    print('End of the session.')
