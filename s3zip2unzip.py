import os
from pathlib import Path
from subprocess import Popen
import boto3
import zipfile

# This script downloads all files from `download_bucket` to the ec2 instance (specified temp dir),
# unzips them on the instance
# creates the output bucket,
# uploads the unzipped file to the new bucket

download_bucket = ...
upload_bucket = ...


FIPS = '11'
TMPDIR = Path("/data/tempdata")

if not TMPDIR.exists():
    TMPDIR.mkdir()


#############

s3 = boto3.client('s3')

# For listing and finding contents of a bucket

# keys = []
# for k in s3.list_objects(Bucket=download_bucket, Prefix='20180107').get('Contents'):
#     if '.zip' in k['Key']:
#         keys.append(k['Key'])


FipsFile = FIPS + '.zip'
Key = '20180107/' + FipsFile
Filename = str(TMPDIR / FipsFile)

# download zip file from s3 to ec2
s3.download_file(
    Bucket=download_bucket,
    Key=Key,
    Filename=Filename
)

# unzip using commandline 
# (to unstall unzip -- `sudo apt-get install zip unzip` on command-line)

p = Popen(["unzip", "-d", str(TMPDIR / FIPS), Filename])

# Create the s3 bucket to upload to.
# be sure the region is the same as the ec2 instance!
s3.create_bucket(
    Bucket=upload_bucket,
    CreateBucketConfiguration={'LocationConstraint': 'us-west-2'}
)


# iterate through newly unzipped files and
# upload each to s3.

basepath = TMPDIR / FIPS
for fp in basepath.rglob('*.txt'):
    keyname = str(fp.relative_to(basepath))
    s3.upload_file(str(fp), upload_bucket, keyname)