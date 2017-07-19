import var
#for i in range(9):
#    print var.load('2017_05_01_0' + str(i) +'_unique_id.pickle')
import findspark
findspark.init()

from pyspark import SparkContext, SparkConf
sc = SparkContext.getOrCreate()
temp = sc.parallelize(var.load('/home/ubuntu/song_user_preference/pickle_output/2016_.pickle')).take(5)
print temp























