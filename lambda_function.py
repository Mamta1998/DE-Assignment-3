import boto3
import pandas as pd

s3_client = boto3.client('s3')
sns_client = boto3.client('sns')
sns_arn = 'arn:aws:sns:us-east-1:339712764669:sns-demo'
destination_bucket = 'aws-de-data'
def lambda_handler(event, context):
    # TODO implement
    print(event)
    try:
        bucket_name = event["Records"][0]["s3"]["bucket"]["name"]
        s3_file_key = event["Records"][0]["s3"]["object"]["key"]
        print(bucket_name)
        print(s3_file_key)
        resp = s3_client.get_object(Bucket=bucket_name, Key=s3_file_key)
        print(resp['Body'])
        

        order_data_df = pd.read_json(resp['Body'],lines=True)
        print("Input dataframe")
        print(order_data_df.head())

        delivered_data_df = order_data_df.loc[(order_data_df['status']=="delivered")]
        print("filtered dataframe")
        print(delivered_data_df)

        print("converting to json file.")
                
        json_output = delivered_data_df.to_json(orient='records',lines=True)
        s3_client.put_object(Bucket=destination_bucket, Key= s3_file_key + '_filtered_data.json', Body=json_output)

        message = "Input S3 File {} has been processed succesfuly and all the delivered order details saved successfully !!".format("s3://"+bucket_name+"/"+s3_file_key)
        respone = sns_client.publish(Subject="SUCCESS - Daily Data Processing",TargetArn=sns_arn, Message=message, MessageStructure='text')
    except Exception as err:
        print(err)
        message = "Input S3 File {} processing is Failed !!".format("s3://"+bucket_name+"/"+s3_file_key)
        respone = sns_client.publish(Subject="FAILED - Daily Data Processing", TargetArn=sns_arn, Message=message, MessageStructure='text')
