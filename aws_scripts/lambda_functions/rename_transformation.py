import json
import boto3
import urllib.parse

s3_client = boto3.client('s3')

def lambda_handler(event, context):
    # Parse the incoming event to get bucket name and file key
    bucket_name = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'])
    
    # Check if the file has a .csv extension and is in the 'transformed' folder
    if key.endswith('.csv') and key.startswith('transformed/'):
        # Define new key with 'transformedcsv/' folder
        new_key = key.replace('transformed/', 'transformedcsv/', 1)
        
        # Copy the object to the new key
        copy_source = {'Bucket': bucket_name, 'Key': key}
        s3_client.copy_object(CopySource=copy_source, Bucket=bucket_name, Key=new_key)
        
        # Delete the old object
        # s3_client.delete_object(Bucket=bucket_name, Key=key)
        
        return {
            'statusCode': 200,
            'body': json.dumps(f'File moved to {new_key} successfully!')
        }
    
    return {
        'statusCode': 200,
        'body': json.dumps('No moving needed for this file.')
    }