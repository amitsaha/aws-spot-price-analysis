import boto3
import os
import sys

client = boto3.client('s3')
paginator = client.get_paginator('list_objects_v2')


def get_page(bucket_name, starting_token=None):
    return paginator.paginate(
       Bucket=bucket_name,
       EncodingType='url',
       PaginationConfig={
        'PageSize': 10,
        'StartingToken': starting_token,
       }
    )

def _download_object(bucket_name, key):
    print(f'Downloading {key}')
    client.download_file(bucket_name, key, f'./data/{key}')


def download(bucket_name=None):

   if not bucket_name:
       bucket_name = os.getenv("BUCKET_NAME")

   if not bucket_name:
       sys.exit("No bucket name found")

   starting_token=None
   while True:
       resp_iterator = get_page(bucket_name, starting_token=starting_token)
       for resp in resp_iterator:
           for key in resp["Contents"]:
               _download_object(bucket_name, key["Key"])

           try:
               page = get_page(bucket_name, starting_token=resp["NextContinuationToken"])
           except KeyError:
               sys.exit(0)


if __name__ == '__main__':
    download()
