# build data lake of NYC taxi data from:
# info: https://medium.com/@NYCTLC

mkdir nyctaxi

# get data
aws s3 cp --recursive --no-sign-request s3://nyc-tlc/ nyctaxi/ #download ~300GB 

# put in hdfs
hdfs dfs -put nyctaxi/* nyctaxi #put in hdfs
