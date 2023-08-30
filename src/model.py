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
import argparse
import os
from utils import get_csv_file_name

parser = argparse.ArgumentParser(description="Predictor")

parser.add_argument( "-f",
                     "--file_name",
                     type=str,
                     help="type file name to save cleaned csv",
                     required=False,
                     default="cleaned_csv",
                     const="cleaned_csv",
                     nargs="?",
                    )

parser.add_argument( "-s",
                     "--save_name",
                     type=str,
                     help="type file name to save csv with predictions",
                     required=False,
                     default="prediction_csv",
                     const="prediction_csv",
                     nargs="?",
                    )

args = parser.parse_args()
file_name = args.file_name
save_name = args.save_name
csv_file_name = get_csv_file_name(file_name)
print(csv_file_name)

spark = SparkSession.builder.appName("k-means").master("local[*]").getOrCreate()

df = spark.read.csv(csv_file_name, inferSchema=True, header=True)
df = df.drop("id")

input_cols = df.columns

vec_assembler = VectorAssembler(inputCols=input_cols, outputCol="features")

final_data = vec_assembler.transform(df)

scaler = StandardScaler(inputCol="features", outputCol="scaled_features", withMean=True, withStd=False)

scaled_df = scaler.fit(final_data).transform(final_data)

kmeans = KMeans(k=4, featuresCol="scaled_features")
model = kmeans.fit(scaled_df)
print(df.columns)

data_to_spark = model.transform(scaled_df).select('prediction', 'packaging_label', 'fat_g', 'carbohydrates_g', 'proteins_g', 'nutrition_score', 'fp_lan', 'fp_lon')
data_to_spark = data_to_spark.withColumn("id", monotonically_increasing_id())

data_to_spark.show()

data_to_spark.repartition(1).write.format("csv").mode('overwrite').save(f"cleaned_data/{save_name}", header='true')

