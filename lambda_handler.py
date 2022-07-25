import time
import urllib
import boto3
import json
from botocore.exceptions import ClientError

client = boto3.resource("dynamodb")
s3 = boto3.client('s3')
glue = boto3.client('glue')
athena = boto3.client('athena')
sns_client = boto3.client('sns')


def lambda_handler(event, context):
    source_bucket = event['Records'][0]['s3']['bucket']['name']
    object_key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'])
    response = s3.get_object(Bucket=source_bucket, Key=object_key)

    size = response['ContentLength']  # file size

    f_extension = object_key.split(".")  # file type

    # reading dynamodb table
    table = client.Table("Configuration")
    Configuration = table.scan()
    # converting json data from table to array
    data = json.dumps(Configuration)
    load_data = json.loads(data)

    if size < int(load_data["Items"][0]['size']) and f_extension[-1] == load_data["Items"][0]['configid']:
        glue_job_csv_to_json("week3job")  # to start the job
        time.sleep(120)  # wait to complete job
        if glue_job_json_to_csv("csvToJsonJob") == "SUCCEEDED":  # start crawler if glue job is successful
            response = glue.start_crawler(Name='week3crawler')
            print(json.dumps(response, indent=4))  # to print json representation data of the db

            # to start athena query
            athena.start_query_execution(
                QueryString='CREATE OR REPLACE VIEW view_name AS '
                            'SELECT id, first_name, last_name, date_of_birth, gender '
                            'FROM ' + f_extension[0],  # f_extension[0] is name of the file
                QueryExecutionContext={
                    'Database': 'week3db'},
                ResultConfiguration={
                    'OutputLocation': 's3://week3-athena-output/', }
            )

        else:
            print("Glue Job Failed")
            # sending alert message from SNS to subscribed e-mail ids
            sns_client.publish(TopicArn='arn:aws:sns:us-east-1:747811223119:gluejob-alert',
                               Message='Alert!! Glue Job Failed',
                               Subject='Glue Job Failed')

    elif size < int(load_data["Items"][0]['size']) and f_extension[-1] == load_data["Items"][0]['configid2']:
        glue_job_json_to_csv("csvToJsonJob")  # to start the job
        time.sleep(120)  # wait to complete job
        if glue_job_json_to_csv("csvToJsonJob") == "SUCCEEDED":  # start crawler if glue job is successful
            response = glue.start_crawler(Name='week3crawler')
            print(json.dumps(response, indent=4))  # to print json representation data of the db

            # to start athena query
            athena.start_query_execution(
                QueryString='CREATE OR REPLACE VIEW view_name AS '
                            'SELECT id, first_name, last_name, date_of_birth, gender '
                            'FROM ' + f_extension[0],  # f_extension[0] is name of the file
                QueryExecutionContext={
                    'Database': 'week3db'},
                ResultConfiguration={
                    'OutputLocation': 's3://week3-athena-output/', }
            )

        else:
            print("Glue Job Failed")
            # sending alert message from SNS to subscribed e-mail ids
            sns_client.publish(TopicArn='arn:aws:sns:us-east-1:747811223119:gluejob-alert',
                               Message='Alert!! Glue Job Failed',
                               Subject='Glue Job Failed')


def glue_job_csv_to_json(job_name):
    try:
        job_run_id = glue.start_job_run(JobName=job_name, Arguments={})
        status_detail = glue.get_job_run(JobName=job_name, RunId=job_run_id.get("JobRunId"))
        status = status_detail.get("JobRun").get("JobRunState")
        return status  # Return Status Running/SUCCEEDED/FAILED.
    except ClientError as e:
        raise Exception("boto3 client error in run_glue_job_get_status: " + e.__str__())
    except Exception as e:
        raise Exception("Unexpected error in run_glue_job_get_status: " + e.__str__())


def glue_job_json_to_csv(job_name):
    try:
        job_run_id = glue.start_job_run(JobName=job_name, Arguments={})
        status_detail = glue.get_job_run(JobName=job_name, RunId=job_run_id.get("JobRunId"))
        status = status_detail.get("JobRun").get("JobRunState")
        return status  # Return Status Running/SUCCEEDED/FAILED.
    except ClientError as e:
        raise Exception("boto3 client error in run_glue_job_get_status: " + e.__str__())
    except Exception as e:
        raise Exception("Unexpected error in run_glue_job_get_status: " + e.__str__())
