import boto3, os, json
from dotenv import load_dotenv
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import gzip
import logging
import threading
import botocore
import gc
load_dotenv()

logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s', level='INFO')

try:
  get_date = datetime.strptime(os.environ['DATE'], "%Y-%m-%d")
except KeyError:
  get_date = datetime.now() - relativedelta(years=1)

profile = os.environ['PROFILE']
name = os.environ['BUCKET']
check_key = os.environ['KEYS']
scan_period = os.environ["SCAN_PERIOD"]
tmp_exist = os.path.exists("./tmp") 

if tmp_exist is False:
  os.system("mkdir tmp")

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
      if "message" in z:
        if check_key in z['message']:
          with open(f"./tmp/{file_name}", 'wb') as data:
            s3.download_fileobj(name, dataname, data)
            break

def get_end_date(start_date, scan_period):
  period_list = scan_period.split("-")
  year,month,day = period_list[0], period_list[1],period_list[2]
  
  days_in_year = 365 * int(year)
  days_in_month = 31 * int(month)
  days = int(day)
  total_days = days_in_year + days_in_month + days
  
  end_date = start_date + timedelta(days=total_days)

  return end_date


if response['ResponseMetadata']['HTTPStatusCode'] == 200:
  buckets = response["Buckets"]
  files = [i for i in buckets if i['Name'] == name]
  if len(files) > 0:
    bucket = files[0]
    format_date = get_date.strftime("%Y/%m/%d")
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
          file_name = keys[i+z].split("/")
          end_date = datetime.date(get_end_date(get_date,scan_period))
          year,month,day = file_name[0],  file_name[1], file_name[2]
          date_str = year+"-"+month+"-"+day
          file_date = datetime.strptime(date_str, '%Y-%m-%d').date()
          
          if file_date < end_date:
            thread = threading.Thread(target=check_data, args=(keys[i+z],))
            thread.start()
            threads.append(thread)
          
            for t in threads:
              t.join()
          else:
            exit()
  else:
    print("Error: Bucket not found")

else:
  print("Error: " + response['ResponseMetadata']['HTTPStatusCode'])