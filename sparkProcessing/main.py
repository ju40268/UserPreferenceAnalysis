from pyspark import SparkContext, SparkConf
from pyspark import SparkFiles
from operator import add
import itertools
import csv
import os
import numpy as np

'''
# Requirement and implementation:

User Geographical distribution

1. How many active users in 2017, and 2016
---> calculate the distinct user id into a list(collect), concatenate all the small list into big list
finally based on the big list to do the distinct().count()

2. How many users per country, City
---> first mapping IP --> country.
checked by 2 approach:
[1] select distintct (id, country) pair. count the occurrence of the key.
[2] using groupByKey. convert into ('country', [userid1, userid2,....]) and count the distinct userid according to the country.
    final get the ('country', user_id_occurence_count)

3. What are the most active users in 2017, and 2016
---> using groupByKey. convert into ('user_id', [count1, count2,....]) and count the distinct userid according to the country.
    final get the ('user_id', user_id_count)

4. What are the most active users since 2010
----> same tech as 3. but counting through 2010

5. What are the total connected user since 2010 to now
----> same tech as 1. but counting through 2010

Concurrent users

1. What is the maximum user online
----> on single time slot, calculate the # of unique user_id. by .distinct().count()

2. What is the maximum user per country
----> including the prob geo 2(?)

3. What is the most popular connection time (system or country)
----> populatr == most people? --> same as con 1 maybe?

App Version

1. How many app version still connecting to the system or 2017, 2016
2. What version has the most users on 2017, 2016
3. What version can find the artist, album, song
4. What app version can display srcInfo, what can not


'''

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
def concurrent_max_transaction_per_country(split_data):
    city_mapping = split_data.map(lambda data: data[4]).mapPartitions(partitionIp2city)
    city_count = city_mapping.map(lambda x:(x,1)).reduceByKey(add)
    sorted_city_count = city_count.sortBy(lambda x: -x[1]).collect()
    # for sorting in the descending order.
    print '#'*40
    return sorted_city_count

# for concurrent max user per country
# https://stackoverflow.com/questions/29717257/pyspark-groupbykey-returning-pyspark-resultiterable-resultiterable
def concurrent_max_user_per_country(split_data):
    id_collect = split_data.map(lambda data: data[3])
    ip_collect = split_data.map(lambda data: data[4]).mapPartitions(partitionIp2city)
    id_ip_pair = ip_collect.zip(id_collect).collect()
    pair = sc.parallelize(id_ip_pair)
    # return the total amount of transaction according to a specific area.
    # print pair.groupByKey().mapValues(len).sortBy(lambda x: -x[1]).collect()
    # print '#'*40
    # just for double-check
    # print pair.groupByKey().map(lambda x : (x[0], set(x[1]))).mapValues(len).sortBy(lambda x: -x[1]).collect()
    # print '#'*40
    # print pair.distinct().keys().take(6)
    distinct_count = pair.distinct().keys().map(lambda x: (x, 1)).reduceByKey(add)
    print distinct_count.sortBy(lambda x: -x[1]).collect()
    print '#'*40
    
def geo_active_user(split_data):
    return split_data.map(lambda data: data[3]).distinct().collect()

def geo_most_active_user(split_data):
    id_count = split_data.map(lambda data: (data[3],1)).reduceByKey(add).collect()
#     for (user_id, count) in id_count:
#         print("%s: %i" % (user_id, count))
    print '#'*40
    return id_count


def list_concat(param_list):
    return sc.parallelize(list(itertools.chain.from_iterable(param_list)))

def make_pair(list1, list2):
    return zip(list1, list2)
    
if __name__ == "__main__":
    f_list = traverse(data_dir)
    Flag_geo_active_user = False
    Flag_geo_most_active_user = True
    Flag_max_user_online = False
    Flag_max_user_per_country = False
    Flag_max_transaction_per_country = False    
    geo_active_user_list = []
    geo_most_active_user_list = []
    concurrent_max_user_online_list = []
    concurrent_max_user_per_country_list = []
    concurrent_max_transaction_per_country_list = []
    
    if Flag_geo_most_active_user:
        for i in range(3):
            split_data = parser(f_list[i])
            print 'filename', f_list[i]
            geo_most_active_user_list.append(geo_most_active_user(split_data))
        flat_most_active = list_concat(geo_most_active_user_list)  
        
        most_active_count = flat_most_active.groupByKey().map(lambda x : (x[0], list(x[1]))).mapValues(sum).sortBy(lambda x: -x[1]).collect() 
        print most_active_count
        
    if Flag_max_transaction_per_country:
        for f in f_list:
            print 'Now parsing with the file:', f
            split_data = parser(f)
            #-----here the most active-----
            concurrent_max_transaction_per_country_list.append(concurrent_max_transaction_per_country(split_data))
        # for summing up
        flat_transaction = list_concat(concurrent_max_transaction_per_country_list)
        print flat_transaction.reduceByKey(add).sortBy(lambda x: -x[1]).collect()
    
    # how many distinct users during all 2016-2017
    if Flag_geo_active_user:
        for f in f_list:
            print 'Now parsing with the file:', f 
            split_data = parser(f)
            geo_active_user_list.append(geo_active_user(split_data))
        flat_geo_active_user = list_concat(geo_active_user_list)
        print flat_geo_active_user.distinct().count()
    
#     if Flag_geo_active_user:
#         for i in range(3):
#             split_data = parser(f_list[i])
#             geo_active_user_list.append(geo_active_user(split_data))
#         flat_geo_active_user = list_concat(geo_active_user_list)
#         print flat_geo_active_user.distinct().count()
    
    
    # for distinct user # on specific time period(1Hr)
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
        for i in range(2):
            split_data = parser(f_list[i])
            concurrent_max_user_per_country_list.append(concurrent_max_user_per_country(split_data))
        #flat_country = list_concat(concurrent_max_user_per_country_list)
        #print flat_country.reduceByKey(add).sortBy(lambda x: -x[1]).collect()
    
