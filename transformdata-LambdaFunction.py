import json
import boto3
import pandas as pd

s3_client = boto3.client('s3')

def lambda_handler(event, context):
    
    source_bucket = event["Records"][0]["s3"]["bucket"]["name"]
    file_name = event["Records"][0]["s3"]["object"]["key"]
    
    target_bucket = "zillowtransformdata"
    target_file_name = file_name[:-5]
    
    response = s3_client.get_object(Bucket=source_bucket, Key=file_name) #This will give me the metadata about my file in the s3 bucket
    data = response['Body'].read().decode('utf-8') #this will give me the json data. Here the body is a streaming object which stores the file data in chunks.
    
    data = json.loads(data) #This line convert the json data into python object such as dictionary
    
    #data = [{}, {}]
        
    output = []
    
    for i in range(len(data)):
        output.append(data[i]["results"])
    
    res = []
    
    for sublist in output:
        for item in sublist:
            res.append(item)

    # output = [ [{}, {}, {}, ], [{}, {}, {}] ]
    
    df = pd.DataFrame(res)

    selected_columns = ['bathrooms', 'bedrooms', 'city', 'homeStatus', 
                    'homeType','livingArea','price', 'rentZestimate','zipcode']
                    
    df = df[selected_columns]
    
    csv_data = df.to_csv(index=False)
    
    bucket_name = target_bucket
    object_key = f"{target_file_name}.csv"
    s3_client.put_object(Bucket=bucket_name, Key=object_key, Body=csv_data)

    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }