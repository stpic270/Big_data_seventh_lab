from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import functions
from pyspark.ml.linalg import Vector
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.ml.feature import StandardScaler
import matplotlib.pyplot as plt
import numpy as np
from pyspark.sql.functions import monotonically_increasing_id
import re
from create_table import get_ip, get_credentials

# Pattern to extract ip to cassandra
pattern =r'(\d+.\d+.\d+.\d+)/\d+'

credentials = get_credentials()
credentials.append(get_ip('test/cassandra_ip.txt'))

spark = SparkSession.builder.\
         appName("cassandra_k-means").\
         config("spark.cassandra.connection.host", str(credentials[2])).\
         config("spark.cassandra.auth.username", str(credentials[0])).\
         config("spark.cassandra.auth.password", str(credentials[1])).\
         master("local[*]").\
         getOrCreate()

df = spark.read.csv("cleaned_OFF.csv", inferSchema=True, header=True)

input_cols = df.columns

vec_assembler = VectorAssembler(inputCols=input_cols, outputCol="features")

final_data = vec_assembler.transform(df)

scaler = StandardScaler(inputCol="features", outputCol="scaled_features", withMean=True, withStd=False)

scaled_df = scaler.fit(final_data).transform(final_data)

kmeans = KMeans(k=4, featuresCol="scaled_features")
model = kmeans.fit(scaled_df)
print(df.columns)

data_to_spark = model.transform(scaled_df).select('prediction', 'packaging', 'fat_g', 'carbohydrates_g', 'proteins_g', 'nutrition_score', 'fp_lat', 'fp_lon')
data_to_spark = data_to_spark.withColumn("id", monotonically_increasing_id())

data_to_spark.write\
        .format("org.apache.spark.sql.cassandra")\
        .mode("append")\
        .options(table="spark1", keyspace="k_means_results")\
        .save()


df = spark.read \
        .format("org.apache.spark.sql.cassandra") \
        .options(table="spark1", keyspace="k_means_results") \
        .load()

df.show()

print("DATA WAS WRITTEN TO AND READ FROM CASSANDRA SUCCESSUFULLY")
