import os
import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError, ClientError

def upload_files_to_s3(directory_path, bucket_name):
    s3 = boto3.client('s3') #you can specify the region you want to use by passing in the region_name parameter (e.g., region_name='us-west-1')

    try:
        # Get a list of all csv files in the directory
        files = [f for f in os.listdir(directory_path) if f.endswith('.csv')]

        for file in files:
            file_path = os.path.join(directory_path, file)

            # Get the file name (without extension) to use as subdirectory name
            base_name = os.path.basename(file_path)
            file_name = os.path.splitext(base_name)[0]

            # Create the path for the file in the S3 bucket
            key = f"{file_name}/{base_name}"

            try:
                # Upload the file
                s3.upload_file(file_path, bucket_name, key)
                print(f"Successfully uploaded {file_path} to {bucket_name}/{key}")
            except (NoCredentialsError, PartialCredentialsError) as e:
                print(f"Credentials error: {e}")
            except ClientError as e:
                print(f"Client error: {e}")
            except Exception as e:
                print(f"Failed to upload {file_path}: {e}")

    except FileNotFoundError as e:
        print(f"Directory not found: {e}")
    except Exception as e:
        print(f"An error occurred: {e}")

# Usage
upload_files_to_s3(r'', '')