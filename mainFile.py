import os
from pathlib import Path
from subprocess import Popen
import boto3
import zipfile

download_bucket = ...
upload_bucket = ...

TMPDIR = Path("/data/tempdata")
FIPS_table = ['2']


for FIPS in FIPS_table:

    #creating connection to the database
    s3 = boto3.client('s3')


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
        CreateBucketConfiguration={'LocationConstraint': 'us-west-1'}
    )


    # iterate through newly unzipped files and
    # upload each to s3.

    basepath = TMPDIR / FIPS
    for fp in basepath.rglob('*.txt'):
        keyname = str(fp.relative_to(basepath))
        s3.upload_file(str(fp), upload_bucket, keyname)