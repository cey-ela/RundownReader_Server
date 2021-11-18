import boto3
import json
from botocore.exceptions import NoCredentialsError, EndpointConnectionError


def upload_to_aws(local_file, bucket, s3_file):
    #with open("C:\\Program Files\\RundownReader_Server\\xyz\\aws_creds.json") as aws_creds:
    with open("/Users/joseedwa/PycharmProjects/xyz/aws_creds.json") as aws_creds:
        aws_credentials = json.load(aws_creds)
        aws_access_key_id = aws_credentials[0]['aws_access_key_id']
        aws_secret_access_key = aws_credentials[0]['aws_secret_access_key']

    s3 = boto3.client('s3',
                      aws_access_key_id=aws_access_key_id,
                      aws_secret_access_key=aws_secret_access_key)

    try:
        s3.upload_file(local_file, bucket, s3_file, ExtraArgs={'ACL':'public-read'})
        return True
    except FileNotFoundError as e:
        print(e)
        return False
    except NoCredentialsError as e:
        print(e)
        return False
    except EndpointConnectionError as e:
        print(e)
        return False
