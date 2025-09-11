import json
import boto3 

s3_client = boto3.client('s3')

def lambda_handler(event, context):
    # TODO implement
    source_bucket = event["Records"][0]["s3"]["bucket"]["name"]
    file_name = event["Records"][0]["s3"]["object"]["key"]
    
    target_bucket = "zillowcopydata"
    copy_source = {"Bucket": source_bucket, "Key": file_name}
    
    waiter = s3_client.get_waiter('object_exists')
    waiter.wait(Bucket = source_bucket, Key=file_name)
    
    s3_client.copy_object(Bucket=target_bucket, Key=file_name, CopySource=copy_source)
    
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
