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

spark = SparkSession.builder.appName("k-means").master("local[1]").getOrCreate()
df = spark.read.csv("cleaned_OFF.csv", inferSchema=True, header=True)

input_cols = df.columns

vec_assembler = VectorAssembler(inputCols=input_cols, outputCol="features")

final_data = vec_assembler.transform(df)

scaler = StandardScaler(inputCol="features", outputCol="scaled_features", withMean=True, withStd=False)

scaled_df = scaler.fit(final_data).transform(final_data)

sse = []
for k in range(2, 11):
    kmeans = KMeans(k=k, featuresCol="scaled_features", maxIter=300)
    model = kmeans.fit(scaled_df)
    wsse_spark = model.summary.trainingCost
    sse.append(wsse_spark)

plt.plot(range(2, 11), sse)
plt.xticks(range(2, 11))
plt.xlabel("Number of Clusters")
plt.ylabel("SSE")
plt.show()
