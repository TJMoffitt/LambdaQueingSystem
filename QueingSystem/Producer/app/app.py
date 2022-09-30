#  Producer - Queing Engine.
#
#  Table Schemea:
# --------------------------------------------------------
# [data1][data2][data3][QueingGroup int][QueingTime Time]
# --------------------------------------------------------
#
#  Daily Group:
#  This engine works by selecting all records for the current days queing groups
#  This queing group is based on the current day. It uses unix time to define
#  day 1 as xxxx-xx-xx and each day forward from that as day 1+x where x
#  is the number of days passed since then. It then takes the modulus of this
#  number to 31 and to define the queing group for the current day. for the
#  current run cycle of 6 days, any policies in this group or the next 5 groups
#  mod 31 are selected as todays policies. eg for group=29 the policies
#  [29, 30, 31, 1, 2, 3].
#
#  Monthly Group:
#  The monthly groups are defined in increasing order from their offset
#  from the last day of the current month, starting at 32. For example
#  if the current month has 30 days, then for a monthly run of 3 days on day 28
#  group 34 will be selected, on day 29 group 33 will be selected and on day 30
#  group 32 will be selected. These policies will run in the same way as the
#  daily policies. However this schema means that the monthly run can be
#  ensured to run at the end of each month rather than being offset by
#  differences in month lengths.
#
#  QueingTime:
#  The polices are sent to an Aws SQS FirstInFirstOut Queue to be run by the
#  consumer script throughout the day. To do this the policies are first ordered
#  by their QueingTime and sent to the Queue with the earlier run times first.
#
#  Un-queue Policies. IGNORE
#  To remove a given policy from queing, multiply its queing group by -1. IGNORE - Set IsUnsubscribed to 1
#  To add this Policy back into queing, repeat this operation.   IGNORE

#
# import time             #
import datetime           #
import pymysql            #
import boto3              #
# import json             #
#
my_session = boto3.session.Session()
my_region = my_session.region_name
ssm = boto3.client("ssm", my_region)
#
#  Config Variables                                                          # Offsets begin with 50, so 50 would be the last day of the month, 51 the second last day of the month ect.
DailyRunCycle = 3                                                            #
MonthlyRunCycle = 3                                                          # days = ['Mon','Tue','Wed','Thu','Fri','Sat','Sun']
MonthylRunDays = ['100', '160']                                               # 101 is the last monday of a month, Sun1 ect is the second to last sunday of a month ect, delta0 is the last day of a month, delta 1 the second to last ect....
DailyRunOffset = 28                                                          # day of week groups begin at 100, second digit is day of week and third is offset, mon=0, tue = 1 ect
queuename = 'PetQueue.fifo'                                                  # eg 101 would be the second to last monday of a month. Needs to be done this way for product spec.
#

client = boto3.client('sqs', my_region)
queue_url = str(client.get_queue_url(QueueName=queuename)['QueueUrl']).replace('https://', 'https://sqs.').replace('queue.', '')


# def TodaysQueingGroups():
#     queingGroups = []
#     unixdate = datetime.datetime(day=1, month=1, year=1970) - datetime.datetime.now()
#     firstgroup = int(unixdate.days-1)%DailyRunOffset + 1
#     for day in range(0,DailyRunCycle):
#         queingGroups.append((firstgroup+day-1)%DailyRunOffset + 1)
#     DaysInCurrentMonth = (datetime.datetime.now().replace(month = datetime.datetime.now().month % 12 + 1, day = 1)-datetime.timedelta(days=1)).day
#     print(DaysInCurrentMonth-int(datetime.datetime.now().day))
#     print(31+DaysInCurrentMonth-int(datetime.datetime.now().day))
#     if DaysInCurrentMonth-int(datetime.datetime.now().day) < MonthlyRunCycle:
#         print(DaysInCurrentMonth-int(datetime.datetime.now().day))
#         queingGroups.append(31+DaysInCurrentMonth-int(datetime.datetime.now().day))
#         print(31+DaysInCurrentMonth-int(datetime.datetime.now().day))
#     print(queingGroups)
#     return queingGroups

def TodaysQueingGroups():
    queingGroups = []
    unixdate = datetime.datetime(day=1, month=1, year=1970) - datetime.datetime.now()  # Switch Around!
    # Daily Queing Group
    firstgroup = int(unixdate.days - 1) % DailyRunOffset
    for day in range(0, DailyRunCycle):
        queingGroups.append((firstgroup + day - 1) % DailyRunOffset)
    DaysInCurrentMonth = (datetime.datetime.now().replace(month=datetime.datetime.now().month % 12 + 1, day=1) - datetime.timedelta(days=1)).day
    # Offset from end of month
    queingGroups.append(50 + DaysInCurrentMonth - int(datetime.datetime.now().day))
    # minus-nth Day of Week, monthly run
    # print(datetime.datetime.strptime(str(datetime.datetime.now()).split(' ')[0], '%Y-%m-%d').weekday())

    monthoffsets = {0: 0, 1: 0, 2: 0, 3: 0, 4: 0, 5: 0, 6: 0}

    for day in range((datetime.datetime.now().replace(month=datetime.datetime.now().month % 12 + 1, day=1) - datetime.timedelta(days=1)).day, 0, -1):
        basemonth = str(datetime.datetime.now()).split(' ')[0].split('-')[0] + '-' + str(datetime.datetime.now()).split(' ')[0].split('-')[1] + '-'
        monthdate = basemonth + str(day).zfill(2)
        if monthdate == str(datetime.datetime.now()).split(' ')[0]:
            dayofweek = datetime.datetime.strptime(monthdate, '%Y-%m-%d').weekday()
            # print(monthoffsets)
            offsetofday = monthoffsets[datetime.datetime.strptime(monthdate, '%Y-%m-%d').weekday()]
        monthoffsets[datetime.datetime.strptime(monthdate, '%Y-%m-%d').weekday()] += 1
    queingGroups.append(int('1' + str(dayofweek) + str(offsetofday)))
    return queingGroups


def FetchRecords(queingGroups):
    connection = my_db_queue()
    with connection.cursor(pymysql.cursors.DictCursor) as cursor:
        querystring = f'select PetPolicyNumber, IsUnsubscribed, QueuingGroup, QueuingTimeOfDay from Pet_Panel.Raw_Panellist where QueuingGroup in({str(queingGroups)[1:-1]}) and IsUnsubscribed = 0 order by QueuingTimeOfDay asc;'
        print(querystring)
        cursor.execute(querystring)
        return cursor.fetchall()


def SendRecords(OrderedRecords):
    sqs = boto3.client('sqs')
    print(queue_url)
    for record in OrderedRecords:
        print(str(record['PetPolicyNumber'] + str(record['QueuingTimeOfDay'])).replace('-', '').replace(': ', '').replace(' ', ''))
        response = sqs.send_message(
            QueueUrl=queue_url,
            DelaySeconds=0,
            MessageGroupId='PetQueue',
            MessageDeduplicationId=str(record['PetPolicyNumber'] + str(record['QueuingTimeOfDay'])).replace('-', '').replace(': ', '').replace(' ', ''),
            MessageAttributes={
                'PetPolicyNumber': {
                    'DataType': 'String',
                    'StringValue': record['PetPolicyNumber']
                },
                'QueuingTimeOfDay': {
                    'DataType': 'String',
                    'StringValue': str(record['QueuingTimeOfDay'])
                }
            },
            MessageBody=(
                f'Queueing record: {record["PetPolicyNumber"]} for {record["QueuingTimeOfDay"]}'
            )
        )
        print(response['MessageId'])


def update_pet_queue_log(PetId, RunTimeScheduled):
    for sitename in ['CON', 'MSM', 'CTM', 'GOCO']:
        connection = my_db_panel()
        with connection.cursor(pymysql.cursors.DictCursor) as cursor:
            querystring = f"""INSERT INTO queing.pet_daily_queue_all
                            (
                            policy,
                            scheduled_run_time,
                            stat,
                            website,
                            date_created,
                            date_updated
                            ) VALUES (
                            '{PetId}',
                            '{RunTimeScheduled}',
                            'stat',
                            '{sitename}',
                            NOW(),
                            NOW());"""
            print(querystring)
            cursor.execute(querystring)
            connection.commit()
            cursor.close()
            connection.close()


def my_db_queue():
    """Database connection details"""

    return pymysql.connect(host=ssm.get_parameter(Name="RDS_HOST_AWS", WithDecryption=True)["Parameter"]["Value"], port=3306, user=ssm.get_parameter(Name="RDS_USER_AWS", WithDecryption=True)["Parameter"]["Value"], passwd=ssm.get_parameter(Name="RDS_PASSWORD_AWS", WithDecryption=True)["Parameter"]["Value"], db="queing")


def my_db_panel():
    """Database connection details"""

    return pymysql.connect(host=ssm.get_parameter(Name="RDS_HOST_AWS", WithDecryption=True)["Parameter"]["Value"], port=3306, user=ssm.get_parameter(Name="RDS_USER_AWS", WithDecryption=True)["Parameter"]["Value"], passwd=ssm.get_parameter(Name="RDS_PASSWORD_AWS", WithDecryption=True)["Parameter"]["Value"], db='Pet_Panel')


def main():
    queingGroups = TodaysQueingGroups()
    orderedRecords = FetchRecords(queingGroups)
    for record in orderedRecords:
        update_pet_queue_log(record['PetPolicyNumber'], record['QueuingTimeOfDay'])
    SendRecords(orderedRecords)
    print('Done')


def handler(event, context):
    main()

#  main()
#  lambda_handler(0, 0)
