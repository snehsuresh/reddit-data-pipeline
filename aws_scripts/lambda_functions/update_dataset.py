import boto3
import json

# Initialize the QuickSight client
client = boto3.client('quicksight')

def lambda_handler(event, context):
    dataset_id = 'd58fc377-b352-45f4-847f-d8cfd037d7d4'
    aws_account_id = '905418253729'  
    data_source_arn = "arn:aws:quicksight:us-east-1:905418253729:datasource/32b8ad52-d3cd-429b-86e1-ee864b2e54c6"

    try:
        response = client.update_data_set(
            AwsAccountId=aws_account_id,
            DataSetId=dataset_id,
            Name='subreddit_music_new',
            PhysicalTableMap={
                'MyPhysicalTable': {
                    'S3Source': {
                        'DataSourceArn': data_source_arn,
                        'UploadSettings': {
                            'Format': 'CSV',
                            'StartFromRow': 1,  # Assuming you want to start from the first row
                            'ContainsHeader': True,
                            'TextQualifier': 'DOUBLE_QUOTE',
                            'Delimiter': ','
                        },
                        'InputColumns': [
                            { "Name": "id", "Type": "STRING" },
                            { "Name": "title", "Type": "STRING" },
                            { "Name": "score", "Type": "STRING" },
                            { "Name": "num_comments", "Type": "STRING" },
                            { "Name": "author", "Type": "STRING" },
                            { "Name": "created_utc", "Type": "STRING" },  # Use "DATE_TIME" if applicable
                            { "Name": "url", "Type": "STRING" },
                            { "Name": "over_18", "Type": "STRING" },
                            { "Name": "artist", "Type": "STRING" },
                            { "Name": "genre", "Type": "STRING" },
                            { "Name": "ESS_updated", "Type": "STRING" }
                        ]
                    }
                }
            },
            ImportMode='SPICE'
        )
        return {
            'statusCode': 200,
            'body': json.dumps('Dataset refreshed successfully')
        }
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps(f'Error refreshing dataset: {str(e)}')
        }
