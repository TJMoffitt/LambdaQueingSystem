#  Consumer Of New Queing Engine.
#
#  The consumer script runs once a minute on aws and retrieves the first record
#  from the aws sqs fifo queue. If the runtime of this policy is before the
#  current time, it will instantiate a scraper instance for all four scrapers
#  using that records information and remove that policy from the sqs queue,
#  it will then repeat this with the next record until reaching a record
#  for which the time is later. At this point the script will terminate.
#  If there are no records, then the script will terminate instantly.

import boto3
import json
import random
# import time
import datetime
import pymysql

my_session = boto3.session.Session()
my_region = my_session.region_name
ssm = boto3.client("ssm", my_region)
queuename = 'PetQueue.fifo'

client = boto3.client('sqs', my_region)
queue_url = str(client.get_queue_url(QueueName=queuename)['QueueUrl']).replace('https://', 'https://sqs.').replace('queue.', '')

petscraperslist = ['PetCONScraper', 'PetMSMScraper', 'PetCTMScraper', 'PetGOCOScraper']


def FetchRecord():
    sqs = boto3.client('sqs')
    response = sqs.receive_message(
        QueueUrl=queue_url,
        AttributeNames=[
            'All',
        ],
        MessageAttributeNames=[
            'PetPolicyNumber', 'QueuingTimeOfDay'
        ],
        MaxNumberOfMessages=1,
        VisibilityTimeout=60,
        WaitTimeSeconds=0,
        ReceiveRequestAttemptId=str(random.randint(0, 10000000000000))
    )
    # print(currenttime)
    # print(response['MessageId'])
    print(response)
    print(response['Messages'][0]['MessageAttributes']['QueuingTimeOfDay']['StringValue'])
    print(response['Messages'][0]['MessageAttributes']['QueuingTimeOfDay']['StringValue'])
    print(response['Messages'][0]['ReceiptHandle'])

    return {
        'QueingTimeOfDay': response['Messages'][0]['MessageAttributes']['QueuingTimeOfDay']['StringValue'],
        'PetPolicyNumber': response['Messages'][0]['MessageAttributes']['PetPolicyNumber']['StringValue'],
        'ReceiptHandle': response['Messages'][0]['ReceiptHandle'],
    }
    # print(response['Messages'][2]['MessageAttributes']['QueuingTimeOfDay']['StringValue'])


def RemoveRecordFromQueue(recordName, ReceiptHandle):
    sqs = boto3.client('sqs')
    sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=ReceiptHandle)


def StartScrapers(record, eventid):
    for functionname in petscraperslist:
        lambda_client = boto3.client('lambda')
        print('connected')
        payloaddict = {'policynumber': record,
                       'runId': eventid}
        response = lambda_client.invoke(
            FunctionName=functionname,
            InvocationType='Event',
            Payload=json.dumps(payloaddict),
        )
        print('Running')
        try:
            print(response['Payload'])
            print(response['Payload'].read().decode("utf-8"))
        except:
            pass


def update_pet_queue_log(PetId):
    connection = my_db_queue()
    with connection.cursor(pymysql.cursors.DictCursor) as cursor:
        querystring = f"""UPDATE queing.pet_daily_queue_all
                        SET
                        stat = 'Running',
                        start_run_time = now()
                        where
                        DATE(date_created) = DATE(NOW()) and
                        policy = '{PetId}'
                        ;"""
        # print(querystring)
        cursor.execute(querystring)
        connection.commit()
        cursor.close()
        connection.close()
        return cursor.fetchall()


def my_db_queue():
    """Database connection details"""
    return pymysql.connect(host=ssm.get_parameter(Name="RDS_HOST_AWS", WithDecryption=True)["Parameter"]["Value"], port=3306, user=ssm.get_parameter(Name="RDS_USER_AWS", WithDecryption=True)["Parameter"]["Value"], passwd=ssm.get_parameter(Name="RDS_PASSWORD_AWS", WithDecryption=True)["Parameter"]["Value"], db="queing")


def main():
    while True:
        lastRecord = FetchRecord()
        currenttime = datetime.datetime.now().strftime("%H:%M:%S")
        if lastRecord['QueingTimeOfDay'] > currenttime:
            break
        else:
            RemoveRecordFromQueue(lastRecord['PetPolicyNumber'], lastRecord['ReceiptHandle'])
            StartScrapers(lastRecord['PetPolicyNumber'], random.randint(0, 100000000))  # Replace With A Run ID
            print('Scraper Running')
            update_pet_queue_log(lastRecord['PetPolicyNumber'])
            print('Monitoring Table Updated')
            print('Last Record Removed')


def handler(event, context):
    main()
