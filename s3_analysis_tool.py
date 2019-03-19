import boto3
import datetime
import pprint
import argparse
import math

parser = argparse.ArgumentParser(description='Process S3 Buckets.')
parser.add_argument('-k', '--kilabytes', help='format file size in megabytes', action='store_true')
parser.add_argument('-m', '--megabytes', help='format file size in megabytes', action='store_true')
parser.add_argument('-g', '--gigabytes', help='format file size in megabytes', action='store_true')
#filter prefix
#parser.add_argument('-g', '--gigabytes', help='format file size in megabytes', action='store_true')

args = parser.parse_args()
size_factor = 0
if args.kilabytes:
  size_factor = 1
elif args.megabytes:
  size_factor = 2
elif args.gigabytes:
  size_factor = 3
## S3 Client
s3client = boto3.client('s3')

## Constants
storage_types = ['StandardStorage','StandardIAStorage','ReducedRedundancyStorage']
now = datetime.datetime.now()

def last_modified_file_and_count(bucket_name):
    print(bucket_name +": starting last_modified_file_and_count") 
    get_last_modified = lambda obj: int(obj['LastModified'].strftime('%s'))
    keys = []

    kwargs = {'Bucket': bucket_name}
    while True:
        resp = s3client.list_objects_v2(**kwargs)
        for obj in resp['Contents']:
            keys.append(obj)
        try:
            kwargs['ContinuationToken'] = resp['NextContinuationToken']
        except KeyError:
            break
    print(bucket_name +": Finished last_modified_file_and_count") 
    return keys[-1]['LastModified'], len(keys)

def cw_bucket_metric(bucket_name, storage_type, metric_name='BucketSizeBytes'):
    bucket_location = s3client.get_bucket_location(Bucket=bucket_name)
    region = bucket_location['LocationConstraint']

    if region is None:
        region = 'us-east-1'  

    cw = boto3.client('cloudwatch', region_name=region)
    # For each bucket item, look up the cooresponding metrics from CloudWatch
    response = cw.get_metric_statistics(Namespace='AWS/S3',
                                        MetricName=metric_name,
                                        Dimensions=[
                                            {'Name': 'BucketName', 'Value': bucket_name},
                                            {'Name': 'StorageType', 'Value': storage_type}
                                        ],
                                        Statistics=['Average'],
                                        Period=3600,
                                        StartTime=(now-datetime.timedelta(days=1)).isoformat(),
                                        EndTime=now.isoformat()
                                        )
    if len(response["Datapoints"]) < 1:
       return 0
    elif len(response["Datapoints"]) > 1:
        raise Exception("response[\"Datapoints\"] count > 1\n" + response['Datapoints'])
    return int(response["Datapoints"][0]["Average"])

allbuckets = s3client.list_buckets()
for bucket in allbuckets['Buckets']:
    #bucket['Last Modified'], bucket['File Count'] = last_modified_file_and_count(bucket['Name']) Too Slow not viable

    bucket['File Count'] = cw_bucket_metric(bucket['Name'], 'AllStorageTypes', 'NumberOfObjects')
    for storage_type in storage_types:
        bucket[storage_type] = cw_bucket_metric(bucket['Name'],storage_type)/math.pow(1000, size_factor)
pprint.pprint(allbuckets['Buckets'])
