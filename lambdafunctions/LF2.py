import json
import boto3
import random
from requests_aws4auth import AWS4Auth
import requests


def sqs_msg():
    sqs = boto3.client('sqs')
    response = sqs.receive_message(
        QueueUrl="https://sqs.us-east-1.amazonaws.com/xxxxxxxxxx/Q1",
        AttributeNames=['SentTimestamp'],
        MaxNumberOfMessages=5,
        MessageAttributeNames=['All'],
        VisibilityTimeout=10,
        WaitTimeSeconds=0
    )
    return response


def es_find_busID(cuisine):
    credentials = boto3.Session().get_credentials()
    auth = AWS4Auth(credentials.access_key, credentials.secret_key,
                    'us-east-1', 'es', session_token=credentials.token)
    host = 'https://search-dining-xxxxxx.us-east-1.es.amazonaws.com'
    index = 'restaurants'
    type = 'Restaurant'
    url = host + '/' + index + '/' + type + '/_search'
    query = {
        "size": 1000,
        "query": {
            "query_string": {
                "default_field": "cuisine",
                "query": cuisine
            }
        }
    }
    headers = {"Content-Type": "application/json"}
    response = requests.get(
        url, auth=auth, headers=headers, data=json.dumps(query))

    response = response.json()
    hits = response['hits']['hits']
    buisinessIDs = []
    for hit in hits:
        buisinessIDs.append(str(hit['_source']['RestaurantID']))
    # print('len: ', len(buisinessIDs))
    return buisinessIDs


def db_find_info(businessIDs):
    client = boto3.resource('dynamodb')
    table = client.Table('yelp-restaurants')

    res = []
    for id in businessIDs:
        response = table.get_item(Key={'Business ID': id})
        res.append(response)
    return res


def format_msg(restaurantsInfo, message):
    people = message['People']
    date = message['Date']
    dining_time = message['Time']
    cuisine = message['Cuisine']

    msg = 'Hello! Here are my {} restaurant suggestions for {} people, for {} at {}: \n'.format(
        cuisine, people, date, dining_time)
    for i, r in enumerate(restaurantsInfo):
        msg += str(i+1) + '. {}, located at {}, rating {}\n'.format(
            r['Item']['name'], ' '.join(r['Item']['Address']), r['Item']['Rating'])
    msg += 'Enjoy your meal!'
    return msg


def send_email(message, email):
    client = boto3.client("ses")
    response = client.send_email(
        Destination={
            "ToAddresses": [
                email,
            ],
        },
        Message={
            "Body": {
                "Text": {
                    "Charset": "UTF-8",
                    "Data": message,
                }
            },
            "Subject": {
                "Charset": "UTF-8",
                "Data": "Dining Suggestions",
            },
        },
        Source="1158288818@qq.com",
    )
    # print(response)


def delete_message(receipt_handle):
    sqs = boto3.client('sqs')
    res = sqs.delete_message(QueueUrl='https://sqs.us-east-1.amazonaws.com/xxxxxx/Q1',
                       ReceiptHandle=receipt_handle
                       )
    print(res)


def lambda_handler(event, context):
# if __name__ == '__main__':
    sqs_response = sqs_msg()
    # print(sqs_response)

    if "Messages" in sqs_response.keys():
        for message in sqs_response['Messages']:
            msg = json.loads(message['Body'])
            cuisine = msg['Cuisine']
            businessIDs = es_find_busID(cuisine)

            # Assume that it returns a list of restaurantsIds
            # random select 5 from the list
            businessIDs = random.sample(businessIDs, 5)
            print('businessIDs :', businessIDs)

            # get info from DynamoDB
            restaurantsInfo = db_find_info(businessIDs)

            # create required message using info
            msgToSend = format_msg(restaurantsInfo, msg)
            print(msgToSend)

            email = msg['Email']
            send_email(msgToSend, email)

            # delete message from sqs
            receipt_handle = message['ReceiptHandle']
            delete_message(receipt_handle)
