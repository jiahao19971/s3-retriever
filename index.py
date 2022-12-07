import boto3, os, json
from dotenv import load_dotenv
from datetime import datetime
from dateutil.relativedelta import relativedelta
import gzip
import logging
import threading
import botocore
import gc
load_dotenv()

logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s', level='INFO')

profile = os.environ['PROFILE']
name = os.environ['BUCKET']
check_key = os.environ['KEYS']

check_key = check_key.split(",")
session = boto3.Session(profile_name=profile)

client_config = botocore.config.Config(
  max_pool_connections=300,
)
# Let's use Amazon S3
s3 = session.client('s3', config=client_config)

response = s3.list_buckets()

def check_data(dataname):
  file_name = dataname.replace("/", ":")
  obj = news3.Object(name, dataname)
  logs = "Checking: " + dataname
  logging.info(logs)
  with open("./runs.log", "a") as d:
    d.write(logs + "\n")
  with gzip.GzipFile(fileobj=obj.get()['Body']) as gzipfile:
    content = gzipfile.read()
    
    decode = content.decode("utf-8")

    splits = decode.splitlines()
          
    exist = []
    for i in splits:
      result = []
      for z in check_key:
        result.append(z in i)
      if all(result):
        exist.append(json.loads(i))

    for z in exist:
      checked_file = z['message']

      if "0" not in checked_file:
        with open(f"./tmp/{file_name}", 'wb') as data:
          s3.download_fileobj(name, dataname, data)
          break

if response['ResponseMetadata']['HTTPStatusCode'] == 200:
  buckets = response["Buckets"]
  files = [i for i in buckets if i['Name'] == name]
  if len(files) > 0:
    bucket = files[0]
    one_year_ago = datetime.now() - relativedelta(years=1)
    format_date = one_year_ago.strftime("%Y/%m/%d")
    keys = []
    objs = s3.list_objects_v2(
      Bucket=bucket['Name'],
      StartAfter=format_date
    )
    keys = [i['Key'] for i in objs['Contents']]
    while objs["IsTruncated"]:
      objs = s3.list_objects_v2(
        Bucket=bucket['Name'],
        StartAfter=format_date,
        ContinuationToken=objs['NextContinuationToken']
      )
      newKey = [i['Key'] for i in objs['Contents']]
      keys = keys + newKey

    with open('alternative.txt', 'w') as data:
      data.write(",".join(keys))
      
    with open("alternative.txt", 'r') as data:
      f = data.read()
      keys = f.split(",")

      news3 = session.resource('s3', config=client_config)
      for i in range(0, len(keys), 10):
        gc.collect()
        threads = []
        for z in range(0, 10):
          thread = threading.Thread(target=check_data, args=(keys[i+z],))
          thread.start()
          threads.append(thread)
          
        for t in threads:
          t.join()
  else:
    print("Error: Bucket not found")

else:
  print("Error: " + response['ResponseMetadata']['HTTPStatusCode'])