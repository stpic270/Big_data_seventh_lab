from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import col,when,lower
from pyspark.sql.functions import split
from pyspark.ml.feature import StringIndexer
from utils import get_ip, get_credentials
import argparse

parser = argparse.ArgumentParser(description="Predictor")

parser.add_argument( "-s",
                     "--file_name",
                     type=str,
                     help="type file name to save cleaned csv",
                     required=False,
                     default="cleaned_csv",
                     const="cleaned_csv",
                     nargs="?",
                    )

parser.add_argument( "-t",
                     "--table_name",
                     type=str,
                     help="type table name",
                     required=False,
                     default="raw_data",
                     const="raw_data",
                     nargs="?",
                    )

parser.add_argument( "-k",
                     "--keyspace_name",
                     type=str,
                     help="type keyspace name",
                     required=False,
                     default="data_k_means",
                     const="data_k_means",
                     nargs="?",
                    )

# Get typed parameters
args = parser.parse_args()
keyspace_name = args.keyspace_name
table_name = args.table_name
file_name = args.file_name

# Pattern to extract ip to cassandra
pattern =r'(\d+.\d+.\d+.\d+)/\d+'

credentials = get_credentials()
credentials.append(get_ip('test/cassandra_ip.txt', pattern))

spark = SparkSession.builder.\
         appName("create_cleaned_dataset").\
         config("spark.cassandra.connection.host", str(credentials[2])).\
         config("spark.cassandra.auth.username", str(credentials[0])).\
         config("spark.cassandra.auth.password", str(credentials[1])).\
         master("local[*]").\
         getOrCreate()

df = spark.read \
        .format("org.apache.spark.sql.cassandra") \
        .options(table=table_name, keyspace=keyspace_name) \
        .load()

df.show()

dict_elements_first_packaging_code_geo = df.groupBy('first_packaging_code_geo').count().sort('count').collect()
# Get most common element (dict_elements_first_packaging_code_geo[-2][0] - string of elemtns from column used above)
most_common_first_packaging_code_geo = dict_elements_first_packaging_code_geo[-2][0]

dict_elements_packaging = df.groupBy('packaging').count().sort('count').collect()
# Get most common element (dict_elements_packaging[-2][0] - string of elemtns from column used above)
most_common_packaging = dict_elements_packaging[-2][0].split(",")[0]

# Change names of some columns
df = df.withColumnRenamed("nutrition_score_fr_100g", "nutrition_score").\
                withColumnRenamed("fat_100g", "fat_g").\
                withColumnRenamed("carbohydrates_100g", "carbohydrates_g").\
                withColumnRenamed("proteins_100g", "proteins_g")

# Fill 'packaging' null columns with the most common word
df = df.na.fill(subset=["packaging"], value=most_common_packaging)

# Fill "first_packaging_code_geo" null columns with the most common word
df = df.na.fill(subset=["first_packaging_code_geo"], value=most_common_first_packaging_code_geo)

# Split "first_packaging_code_geo" column on 2 cause it has 2 parameters in one column
split_col = split(df['first_packaging_code_geo'], ',')
df = df.withColumn('fp_lan', split_col.getItem(0))
df = df.withColumn('fp_lon', split_col.getItem(1))
# Cast new parameters to double
df = df.withColumn("fp_lan",col("fp_lan").cast("double"))
df = df.withColumn("fp_lon",col("fp_lon").cast("double"))
# Drop "first_packaging_code_geo" column and null rows
df = df.drop("first_packaging_code_geo")
df = df.na.drop("any")

# Lower the first capital in 'packaging' column
df = df.withColumn("packaging", lower(col("packaging")))
# Get labels (indexes) for packing column
indexer = StringIndexer(inputCol="packaging", outputCol="packaging_label")
df = indexer.fit(df).transform(df)
# Cast new labels to integer
df = df.withColumn("packaging_label",col("packaging_label").cast("integer"))
# Drop packaging column
df = df.drop("packaging")

df.show()
print(df.count())

# Save cleaned data
df.repartition(1).write.format("csv").mode('overwrite').save(f"cleaned_data/{file_name}", header='true')
