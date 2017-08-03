import json
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
sc = SparkContext('local')
#spark = SparkSession(sc)
# --------------
#json_text = 'calculated_output.json'
#parsed = json.loads(json_text)
#df = spark.createDataFrame(parsed)
#display(df)
df = sc.wholeTextFiles('calculated_output.json').flatMap(lambda x: json.loads(x[1])).take(10)
print df
#display(df)