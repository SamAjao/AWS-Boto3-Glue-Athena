import pandas as pd
import os
import re
import time
import logging
import boto3
from botocore.exceptions import ClientError

# filetype = 'csv'
filetype = 'parquet'

path = '/Users/josephcline/Downloads' ## <---------- Add your path
repat = re.compile('\\.tsv$')
bucket = 'pt.glue.athena.demo'  ## <----- Add your S3 bucket


def s3_upload_file(file_name, bucket, object_name):
    # Modified from Boto3 documentation
    """Upload a file to an S3 bucket
    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """

    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = file_name

    s3_client = boto3.client('s3')
    try:
        response = s3_client.upload_file(file_name, bucket, object_name)
    except ClientError as err:
        logging.error(err)
        return False
    return True


for root, dirs, files in os.walk(path):
    for file in files:
        if repat.search(file):
            try:
                file_root_name = file[:-4]  ##file name without extension
                oldfile = os.path.join(root, file)

                dataDf = pd.read_csv(oldfile, sep='\t', dtype=object, na_values='\\N', quotechar='"')
                dataDf = dataDf.apply(lambda x: x.str.replace('[^a-zA-Z0-9\\"\\,]', ''), axis=0)

                ## for csv files
                if filetype == 'csv':
                    csv_file = file_root_name + '.csv'
                    csv_bucket = '{0}/{1}/{2}'.format('imdb_csv', file_root_name, csv_file)
                    newfile_csv = os.path.join(root, csv_file)
                    dataDf.to_csv(newfile_csv, sep='\t', header=True, index=False, quotechar='"')
                    s3_upload_file(newfile_csv, bucket, csv_bucket)
                    print('Uploaded file: {0} at {1}'.format(newfile_csv, time.asctime(time.localtime())))

                ## for parquet files
                if filetype == 'parquet':
                    parquet_file = file_root_name + '.parquet'+'.gz'
                    parquet_bucket = '{0}/{1}/{2}'.format('imdb_parquet', file_root_name, parquet_file)
                    newfile_parquet = os.path.join(root, parquet_file)
                    dataDf.to_parquet(newfile_parquet, compression='gzip')
                    s3_upload_file(newfile_parquet, bucket, parquet_bucket)
                    print('Uploaded file: {0} at {1}'.format(newfile_parquet, time.asctime(time.localtime())))

            except ValueError as e:
                print('Error creating CSV file for {0}: {1}'.format(file, str(e)))



