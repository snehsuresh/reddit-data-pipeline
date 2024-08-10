import json
import boto3

s3_client = boto3.client("s3")


def lambda_handler(event, context):
    # Parse the incoming event to get bucket name
    bucket_name = event["Records"][0]["s3"]["bucket"]["name"]

    # List objects in the 'transformed/' folder
    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix="transformed/")

    # Check if there are any objects in the 'transformed/' folder
    if "Contents" not in response:
        return {"statusCode": 200, "body": json.dumps("No files to process.")}

    # Filter objects that start with 'run'
    filtered_files = [
        obj for obj in response["Contents"] if obj["Key"].startswith("transformed/run")
    ]

    if not filtered_files:
        return {
            "statusCode": 200,
            "body": json.dumps('No files starting with "run" to process.'),
        }

    # Get the most recent file (assuming the list is ordered by upload time)
    last_file = sorted(filtered_files, key=lambda x: x["LastModified"], reverse=True)[0]
    key = last_file["Key"]

    # Define new key with 'transformedcsv/' folder and handle extension
    new_key = key.replace("transformed/", "transformedcsv/", 1)
    if not new_key.endswith(".csv"):
        new_key += ".csv"

    # Copy the object to the new key
    copy_source = {"Bucket": bucket_name, "Key": key}
    s3_client.copy_object(CopySource=copy_source, Bucket=bucket_name, Key=new_key)

    # Delete the old object
    # s3_client.delete_object(Bucket=bucket_name, Key=key)

    return {
        "statusCode": 200,
        "body": json.dumps(f"File moved to {new_key} successfully!"),
    }
