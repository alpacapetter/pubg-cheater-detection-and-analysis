from pyspark.sql import SparkSession
from pyspark import SparkContext

spark = SparkSession.builder.appName('pubg').getOrCreate()