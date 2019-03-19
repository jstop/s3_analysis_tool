import boto3
import datetime
import pprint
import argparse
import math
import json
import urllib2

parser = argparse.ArgumentParser(description='Process S3 Buckets.')
parser.add_argument('-k', '--kilabytes', help='format file size in kilabytes', action='store_true')
parser.add_argument('-m', '--megabytes', help='format file size in megabytes', action='store_true')
parser.add_argument('-g', '--gigabytes', help='format file size in gigabytes', action='store_true')
parser.add_argument('-t', '--terabytes', help='format file size in terabytes', action='store_true')
#filter prefix
#parser.add_argument('-f', '--filter', help='filter: folders to include in scan of s3 buckets', action='store_true')

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

now = datetime.datetime.now()
response = urllib2.urlopen('http://aws.amazon.com/s3/pricing/pricing-storage.json')
pricejson = response.read()
PRICING = json.loads(pricejson)

storage_types = ['StandardStorage','StandardIAStorage','ReducedRedundancyStorage', 'GlacierStorage']
storage_type_pricings = {'StandardStorage': 'storage', 'StandardIAStorage': 'infrequentAccessStorage', 'ReducedRedundancyStorage': 'reducedRedundancyStorage', 'GlacierStorage': 'glacierStorage'}

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


def cost(storage_type, gigabytes):
    cost = 0
    position = {'storage': 0, 'infrequentAccessStorage': 1, 'glacierStorage': 2}
    STORAGE_TYPE_PRICINGS = {'StandardStorage': 0, 'StandardIAStorage': 1, 'ReducedRedundancyStorage': 1, 'GlacierStorage': 2}
    storage_position  = STORAGE_TYPE_PRICINGS[storage_type]

    if gigabytes <= 50000:
        cost += gigabytes * float(PRICING['config']['regions'][0]['tiers'][0]['storageTypes'][storage_position]['prices']['USD'])
    elif gigabytes > 50000 and gigabytes <= 500000:
        cost += 50000 * float(PRICING['config']['regions'][0]['tiers'][1]['storageTypes'][storage_position]['prices']['USD'])
        cost += (gigabytes - 50000) * float(PRICING['config']['regions'][0]['tiers'][1]['storageTypes'][storage_position]['prices']['USD'])
        gigabytes = gigabytes - 50000
    elif gigabytes > 500000:
        cost += 50000 * float(PRICING['config']['regions'][0]['tiers'][0]['storageTypes'][storage_position]['prices']['USD'])
        cost += 450000 * float(PRICING['config']['regions'][0]['tiers'][1]['storageTypes'][storage_position]['prices']['USD'])
        cost += (gigabytes - 500000) * float(PRICING['config']['regions'][0]['tiers'][0]['storageTypes'][storage_position]['prices']['USD'])
    return cost


# Main Execution

allbuckets = s3client.list_buckets()
for bucket in allbuckets['Buckets']:

    # LastModified
    # This worked but is way to slow for buckets with a lot of files
    # I think a good work around option would be to trigger a lambda to write to dynamodb or the s3 bucket itself with this sort of bucket metadata
    # I looked for work arounds but last modified file isn't a natively produced figure and either needs to be written on writes to dynamodb or a metadatafile in s3
    #bucket['Last Modified'], bucket['File Count'] = last_modified_file_and_count(bucket['Name']) Too Slow not viable

    bucket['File Count'] = cw_bucket_metric(bucket['Name'], 'AllStorageTypes', 'NumberOfObjects')
    for storage_type in storage_types:
        storage_type_bytes = cw_bucket_metric(bucket['Name'],storage_type)
        storage_type_gigabytes = storage_type_bytes/math.pow(1000,3)
        bucket[storage_type] = storage_type_bytes/math.pow(1000, size_factor)
        bucket[storage_type +'Cost'] = cost(storage_type,storage_type_gigabytes)


pprint.pprint(allbuckets['Buckets'])
