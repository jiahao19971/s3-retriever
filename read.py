import gzip, os, json
from dotenv import load_dotenv

load_dotenv()

check_key = os.environ['KEYS']
check_key = check_key.split(",")
folders = [ f.path for f in os.scandir("./tmp")]
for folder in folders:
  with gzip.GzipFile(folder) as gzipfile:
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

      for i in exist:
        if "message" in i:
          for key in check_key:
            if key in z['message']:
              print(i['message'])