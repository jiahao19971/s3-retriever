## This repo is used to retrieve 1 years logs of data from s3 buckets.
### This is only meant to read the format for scalyr logs. 
### Please update it to your own needs

## Prerequisite
1. Make to have python >= 3.8.5
2. `pip install -r requirements.txt`
3. [Create a .env](#env)

### .env
```diff
+  By default, this file should be stored in ./.env

- The params is case sensitive, please do not change the capitalization

+ BUCKET is a required variable that is pointing to which bucket
BUCKET=

+ PROFILE is the AWS PROFILE that you login with and have access to the s3 bucket
PROFILE=

+ KEYS is the checked variable you need to run through the scalyr logs
+ This is being comma separated, so if u have multiple keys that you need to check 
+ just add in comma will do between the key
KEYS=

+ Date is an optional parameter use to customize the start date the scan should start. 
+ It is format as YYYY-MM-DD or else it will fail
+ By default, it is using 1 year date (today minus 365 days)
DATE=

+ Scan period is how long the logs will be scan through
+ Format is YY-MM-DD, for example we want scan 3 days only it will be 00-00-03
SCAN_PERIOD=
```

To run the script
`python index.py`
