from pyspark import SparkContext, SparkConf
from pyspark import SparkFiles
from operator import add
import itertools
import csv
import os
import numpy as np
# for the visualization
# https://www.datawrapper.de/map/BBqiS/visualize
# http://datamaps.github.io/
#-------------------------------------------------------
# for the geographic mapping using GeoDB
import geoip2.database
from os.path import expanduser
#-------------------------------------------------------
home = expanduser("~")
data_dir = home +'/song_user_preference/' + 'data/'

def traverse(data_dir):
    print('traversing all the file...')
    print('#'*40)
    file_list = []
    for filename in os.listdir(data_dir):
        if filename.endswith(".log"):
            file_list.append(data_dir + filename)
    return file_list

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

def parser(filename):
    lines = sc.textFile(filename)
    split_data = lines.flatMap(lambda line: line.split('\n')).map(lambda line: line.split(' '))
    return split_data

# for concurrent user amount counting.
def concurrent_max_user_online(split_data):
    return split_data.map(lambda data: data[3]).distinct().count()
    
# for concurrent max user per country
# https://stackoverflow.com/questions/33706408/how-to-sort-by-value-efficiently-in-pyspark
# https://stackoverflow.com/questions/30787635/takeordered-descending-pyspark
def concurrent_max_user_per_country(split_data):
    city_mapping = split_data.map(lambda data: data[4]).mapPartitions(partitionIp2city)
    city_count = city_mapping.map(lambda x:(x,1)).reduceByKey(add)
    sorted_city_count = city_count.sortBy(lambda x: -x[1]).collect()
    # for sorting in the descending order.
    for (city, count) in sorted_city_count:
        print("%s: %i" % (city, count))
    print('#'*40)
    return city_count.collect()

def list_concat(param_list):
    return sc.parallelize(list(itertools.chain.from_iterable(param_list)))

def make_pair(list1, list2):
    return zip(list1, list2)
    
if __name__ == "__main__":
    Flag_max_user_per_country = False
    Flag_max_user_online = True
    
    f_list = traverse(data_dir)
     
    concurrent_max_user_online_list = []
    concurrent_max_user_per_country_list = []
    if Flag_max_user_online:
        for f in f_list:
            split_data = parser(f)
            concurrent_max_user_online_list.append(concurrent_max_user_online(split_data))
        concurrent_max_user_online_pair = make_pair(f_list,concurrent_max_user_online_list)
        # only recording for the # of concurrent users, w/o timestamp.
        print concurrent_max_user_online_list 
#         printing = np.array(concurrent_max_user_online_pair)
#         print printing[:,1]
        
        
    if Flag_max_user_per_country:
        for i in range(len(f_list)):
            split_data = parser(f_list[i])
            concurrent_max_user_per_country_list.append(concurrent_max_user_per_country(split_data))
        flat_country = list_concat(concurrent_max_user_per_country_list)
        print flat_country.reduceByKey(add).sortBy(lambda x: -x[1]).collect()
    

