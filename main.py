from pyspark import SparkContext, SparkConf
from pyspark import SparkFiles
from operator import add
import csv
#-------------------------------------------------------
# for the geographic mapping using GeoDB
import geoip2.database
from os.path import expanduser
#-------------------------------------------------------
home = expanduser("~")

def partitionIp2city(iter):
    from geoip2 import database

    def ip2city(ip):
        try:
           city = reader.city(ip).country.name
        except:
            city = 'not found'
        return city

    reader = database.Reader(SparkFiles.get(geoDBpath))
    return [ip2city(ip) for ip in iter]

filename = '00.eu-w1.apps002.log'
lines = sc.textFile(filename)
split_data = sc.textFile(filename).flatMap(lambda line: line.split('\n')).map((lambda line: line.split(' ')))
#------------------------------------------------------------
# for non-overlapping users.
distinct_user = split_data.map(lambda data: data[3]).distinct().collect()
print len(distinct_user)
#------------------------------------------------------------
# for user id
user_counts = split_data.map(lambda data: (data[3],1)).reduceByKey(add)
output_user = user_counts.collect()
#------------------------------------------------------------
city_mapping = split_data.map(lambda data: data[4]).mapPartitions(partitionIp2city)
city_count = city_mapping.map(lambda x:(x,1)).reduceByKey(add)
output_city = city_count.collect()
for (city, count) in output_city:
    print("%s: %i" % (city, count))

with open('output_user.csv', 'wb') as myfile:
    wr = csv.writer(myfile, quoting=csv.QUOTE_ALL)
    wr.writerow(output_city)
print('#'*30)
for (user_id, count) in output_user:
    print("%s: %i" % (user_id, count))

