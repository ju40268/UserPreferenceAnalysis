import json
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
sc = SparkContext('local')
df = sc.wholeTextFiles('calculated_output.json').flatMap(lambda x: json.loads(x[1])).take(10)