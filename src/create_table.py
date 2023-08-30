from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
import os
import re
import cassandra
import time
import json
import argparse
from pyspark.sql import SparkSession
from utils import get_credentials, get_ip, get_query, executions, get_csv_file_name

parser = argparse.ArgumentParser(description="Predictor")

parser.add_argument( "-d",
                     "--data_csv",
                     type=str,
                     help="type name of csv file or csv directory(after written csv file with pyspark)",
                     required=True,
                     default="final.csv",
                     const="final.csv",
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

parser.add_argument( "-tqi",
                     "--table_query_index",
                     type=int,
                     help="type index for table query",
                     required=False,
                     default=1,
                     const=1,
                     nargs="?",
                    )

def create_table(table_name, keyspace_name, table_query_index):

  # Get query for creating keyspace
  q = get_query(pattern="CREATE KEYSPACE IF NOT EXISTS WITH REPLICATION={'class':'SimpleStrategy', 'replication_factor':1};",
                name_insert=keyspace_name,
                insert_index=5)

  # Create keyspace
  executions(session, q)
  
  # Connect to keyspace
  executions(session, f"USE {keyspace_name};")
  
  # Get all lines from table_pattern file
  with open('table_pattern.txt') as f:
    lines = f.readlines()
    f.close()

  pattern = lines[table_query_index]
  
  # clean pattern
  pattern = pattern.replace('\n', '')

  # Get query for creating table
  q = get_query(pattern=pattern,
                name_insert=table_name,
                insert_index=5)

  # Create table
  executions(session, q)

# Pattern to extract ip to cassandra
pattern =r'(\d+.\d+.\d+.\d+)/\d+'

# Get_credentials
credentials = get_credentials()

# Get ip from get_ip function
credentials.append(get_ip('test/cassandra_ip.txt', pattern))

# Past credentials
auth_provider = PlainTextAuthProvider(username=credentials[0], password=credentials[1])

# Connect to cassandra
flag=True
while flag==True:
  try:
    cluster = Cluster([credentials[2]], port=9042, auth_provider=auth_provider)
    session = cluster.connect()
    flag = False
  except cassandra.cluster.NoHostAvailable as er:
    print(er)
    print('This time cassandra did not answer, program will sleep for 40s and  try again')
    time.sleep(40)

# Get typed parameters
args = parser.parse_args()
file_name = args.data_csv
keyspace_name = args.keyspace_name
table_name = args.table_name
table_query_index = args.table_query_index

create_table(table_name=table_name,
             keyspace_name=keyspace_name,
             table_query_index=table_query_index)

print("KEYSPACE AND TABLE WERE CREATED")

# Shut session and cluster
session.shutdown()
cluster.shutdown()
# Connect spark with cassandra
spark = SparkSession.builder\
         .appName("load_data_to_cassandra")\
         .config("spark.cassandra.connection.host", str(credentials[2]))\
         .config("spark.cassandra.auth.username", str(credentials[0]))\
         .config("spark.cassandra.auth.password", str(credentials[1]))\
         .master("local[*]")\
         .getOrCreate()

if os.path.isfile(f"{file_name}"):
  df = spark.read.csv(f"{file_name}", inferSchema=True, header=True)
else:
  csv_file_name = get_csv_file_name(file_name)
  df = spark.read.csv(csv_file_name, inferSchema=True, header=True) 

if "_c0" in df.columns and "nutrition-score-fr_100g" in df.columns:
  # Rename column to match table query 
  df = df.withColumnRenamed("_c0", "id")
  df = df.withColumnRenamed("nutrition-score-fr_100g", "nutrition_score_fr_100g")

# Write data to cassandra
df.write\
  .format("org.apache.spark.sql.cassandra")\
  .mode("append")\
  .options(table=table_name, keyspace=keyspace_name)\
  .save()

# Read previous written data
df = spark.read \
          .format("org.apache.spark.sql.cassandra") \
          .options(table=table_name, keyspace=keyspace_name) \
          .load()

df.show()

print("DATA WAS WRITTEN TO AND READ FROM CASSANDRA SUCCESSUFULLY")