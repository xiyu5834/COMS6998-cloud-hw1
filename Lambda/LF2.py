import boto3
from boto3 import resource
import random
from botocore.exceptions import ClientError
from requests_aws4auth import AWS4Auth
from opensearchpy import OpenSearch, RequestsHttpConnection


def get_restaurant_suggestion(cuisine, client):
    response = client.search(index="restaurants", body={"query": {"match": {"cuisine": cuisine}}})
    restaurants = []
    for entry in response['hits']['hits']:
        restaurants.append(entry["_source"])

    ids = []
    for i in restaurants:
        ids.append(i.get("id"))

    return random.choice(ids)


def get_restaurant_details(restaurantId, table):
    response = table.get_item(Key={'BusinessID': restaurantId})
    name = response["Item"]["name"]
    address = response["Item"]["Address"]
    details = "{}, located at {}.".format(name, address)
    return details


def get_sqs_msgs(sqs, queue_url):
    response = sqs.receive_message(
        QueueUrl=queue_url,
        MaxNumberOfMessages=10,
        AttributeNames=[
            'All'
        ],
        MessageAttributeNames=[
            'All'
        ],
        VisibilityTimeout=30,
        WaitTimeSeconds=0)
    print("Response:--------")
    print(response)
    if "Messages" in response.keys():
        messages = response['Messages']
    else:
        messages = []
    return messages

def send_email(email_address, msg):
    SENDER = "Reservation_System <xy2483@columbia.edu>"
    RECIPIENT = email_address
    AWS_REGION = "us-east-1"
    SUBJECT = "Recommendation_Service"
    BODY_TEXT = (msg)
    CHARSET = "UTF-8"
    client = boto3.client('ses', region_name=AWS_REGION)

    try:
        response = client.send_email(
            Destination={
                'ToAddresses': [
                    RECIPIENT,
                ],
            },
            Message={
                'Body': {
                    'Text': {
                        'Charset': CHARSET,
                        'Data': BODY_TEXT,
                    },
                },
                'Subject': {
                    'Charset': CHARSET,
                    'Data': SUBJECT,
                },
            },
            Source=SENDER,
        )

    except ClientError as e:
        print(e.response['Error']['Message'])
    else:
        print("Email sent! Message ID:"),
        print(response['MessageId'])


def lambda_handler(event, context):

    access_key = "********************"
    secret_key = "****************************************"
    dynamodb = resource('dynamodb', region_name='us-east-1', aws_access_key_id=access_key,
                        aws_secret_access_key=secret_key)
    dynamodb_table = dynamodb.Table('yelp-restaurants')
    host = "search-restaurants-wfx7vnh776iswobhxqwo3zq7fa.us-east-1.es.amazonaws.com"
    region = 'us-east-1'
    service = "es"
    awsauth = AWS4Auth(access_key, secret_key, region, service)
    es_client = OpenSearch(hosts=[{'host': host, 'port': 443}],
                           http_auth=awsauth, use_ssl=True,
                           verify_certs=True, connection_class=RequestsHttpConnection
                           )
    sqs_client = boto3.client('sqs')
    queue_url = "https://sqs.us-east-1.amazonaws.com/292582700758/testQueue"
    sqs_msgs = get_sqs_msgs(sqs_client, queue_url)
    print(event)
    print('------')
    print(sqs_msgs)
    for msg in sqs_msgs:
        cuisine = msg['MessageAttributes']['cuisine']['StringValue']
        date = msg['MessageAttributes']['date']['StringValue']
        numPeople = msg['MessageAttributes']['number']['StringValue']
        phone_num = msg['MessageAttributes']['phone']['StringValue']
        time = msg['MessageAttributes']['time']['StringValue']
        email = msg['MessageAttributes']['email']['StringValue']
        suggestion_res_id = get_restaurant_suggestion(cuisine, es_client)
        res_details = get_restaurant_details(suggestion_res_id, dynamodb_table)
        email_msg = "Hello! Here is my {} restaurant suggestion for {} people, for {} at {} : {}".format(cuisine,
                                                                                                         numPeople,
                                                                                                         date, time,
                                                                                                        res_details)
        
        print(email_msg)
        send_email(email, email_msg)
        receipt_handle = msg['ReceiptHandle']
        sqs_client.delete_message(
            QueueUrl=queue_url,
            ReceiptHandle=receipt_handle)
