import boto3
s3 = boto3.resource('s3')
bucket = s3.Bucket('aws21-squeezebox-analysis-output')
bucket.download_file('calculated_output.json', 'calculated_output.json')