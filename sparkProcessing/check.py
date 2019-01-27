import var
import findspark
findspark.init()

from pyspark import SparkContext, SparkConf
sc = SparkContext.getOrCreate()
output = sc.parallelize(var.load('/home/ubuntu/song_user_preference/pickle_output/2016_.pickle')).take(5)























