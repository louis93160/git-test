import os
import findspark
import matplotlib.pyplot as plt

from pyspark import SparkContext
from pyspark.sql import SQLContext

# Spécifie le chemin où est stocké Spark
os.environ["SPARK_HOME"] = "/home/louis/spark-3.5.0-bin-hadoop3"

# Trouve les exécutables dans le dossier SPARK_HOME
findspark.init()  

# Crée un SparkContext local
sc = SparkContext(master="local[*]")

# Instancie un SQLContext
sql_c = SQLContext(sc)

rdd = sc.parallelize([i for i in range(10)])

df = sql_c.createDataFrame(rdd.map(lambda i: (i,)))
df.show()

df.toPandas().head()

data = sql_c.read.option("header", True).csv("logements.csv")
data.show()

