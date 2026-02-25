import boto3
import json

def lambda_handler(event, context):
    glue = boto3.client('glue', region_name='us-east-1')
    
    job_name = 'nyc-taxi-etl'
    
    try:
        response = glue.start_job_run(JobName=job_name)
        job_run_id = response['JobRunId']
        
        print(f"Successfully triggered Glue job: {job_name}")
        print(f"Job Run ID: {job_run_id}")
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': f'Glue job {job_name} triggered successfully',
                'jobRunId': job_run_id
            })
        }
    except Exception as e:
        print(f"Error triggering Glue job: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e)
            })
        }
