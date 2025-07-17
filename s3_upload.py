import boto3
import os
from botocore.exceptions import ClientError
from botocore.config import Config
import datetime
from dotenv import load_dotenv

load_dotenv()

SPACES_NAME = "purvaj-panda-qc"
REGION = "blr1"
ENDPOINT_URL = f"https://{REGION}.digitaloceanspaces.com"

s3_client = boto3.client(
    "s3",
    endpoint_url=ENDPOINT_URL,
    aws_access_key_id=os.getenv("aws_access_key_id"),
    aws_secret_access_key=os.getenv("aws_secret_access_key"), 
    config=Config(signature_version="s3v4")
)

def upload_to_s3(file, s3_path):
    try:
        s3_client.upload_fileobj(
            file,
            SPACES_NAME,
            s3_path,
            ExtraArgs={'ContentType': file.content_type}
        )
        return {"success": True}
    except Exception as e:
        print(f"Error uploading to S3: {str(e)}")
        return {"success": False, "message": str(e)}

def get_image_list_from_s3(folder_path):
    try:
        print(f"get_image_list_from_s3: Listing objects with prefix '{folder_path}' in bucket '{SPACES_NAME}' - {datetime.datetime.now()}")
        response = s3_client.list_objects_v2(Bucket=SPACES_NAME, Prefix=folder_path)
        image_list = []
        contents = response.get('Contents', [])
        print(f"get_image_list_from_s3: Found {len(contents)} objects for prefix '{folder_path}' - {datetime.datetime.now()}")

        for content in contents:
            key = content['Key']
            print(f"get_image_list_from_s3: Examining key '{key}' - {datetime.datetime.now()}")
            if key.lower().endswith(('.jpg', '.jpeg', '.png')):
                filename = os.path.basename(key)
                image_list.append(filename)
                print(f"get_image_list_from_s3: Added filename '{filename}' - {datetime.datetime.now()}")

        if not image_list:
            print(f"get_image_list_from_s3: No images found for prefix '{folder_path}' - {datetime.datetime.now()}")

        return image_list
    except Exception as e:
        print(f"get_image_list_from_s3 error for prefix '{folder_path}': {str(e)} - {datetime.datetime.now()}")
        return []