import re
import os 
import cassandra
import time 

def executions(ses, q):
  flag=True
  while flag==True:
    try:
      ses.execute(q)
      flag = False
    except cassandra.OperationTimedOut as er:
      print(er)
      print(f'This time cassandra did not answer to the following querry: {q}, program will sleep for 10s and try again')
      time.sleep(10)

# Extraction of credentials
def get_credentials():
  credentials = []
  l, p = 'login:', 'password:'
  with open('test/cassandra_config.txt', 'r') as f:
    for line in f:
      s = line.strip()
      if l in s:
        le = len(l)
        credentials.append(line.strip()[le:])
      if p in s:
        le = len(p)
        le2 = len(credentials[0])
        credentials.append(line.strip()[le:le+le2])
    f.close
  return credentials

# Get ip
def get_ip(path, pattern):
  with open(path, 'r') as ip:
    for line in ip:
      if '172.' in line:
        sp = re.findall(pattern, line)
        return sp[0]

# Return clean query fro cassandra
def get_query(pattern, name_insert, insert_index=5):

  name_insert = name_insert.lower()
  s = pattern.split()
  s.insert(insert_index, f'{name_insert}')
  s = ' '.join(s)

  return s

def get_csv_file_name(file_name):
  
  directory_path = os.path.join("cleaned_data", file_name)
  
  for f in os.listdir(directory_path):
    file_path = os.path.join(directory_path, f)
   
    if ".csv" in file_path and ".crc" not in file_path:
      return file_path

  return None
