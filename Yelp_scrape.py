import requests
import json
from datetime import datetime
from boto3 import resource
from decimal import *
from requests_aws4auth import AWS4Auth
from opensearchpy import OpenSearch, RequestsHttpConnection


API_KEY = 'doT9nmte7z-_9f9uv4bMgbH4CVtRm1MujeCylGpNf3ehoGN6miqoK1zmQGoFZ9wvjLkNydFJNMLN0ZohYx__ANBChJ9ujuf1kqiHU4TUAz3JS6LAFlCpm7Xi49ESYnYx'
headers = {'Authorization': 'Bearer %s' % API_KEY}
url='https://api.yelp.com/v3/businesses/search'

access_key = "AKIA5OFFSXJ5DTZ6FI5T"
secret_key = "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"

dynamodb = resource('dynamodb', region_name='us-east-1', aws_access_key_id=access_key,  aws_secret_access_key = secret_key)
dynamodb_table = dynamodb.Table('yelp-restaurants')

def format_data(data, cuisine, items_list, restaurants_set):
    for item in data:
        if item['alias'] in restaurants_set:
            continue
        dic = {}
        dic['BusinessID']=item['id']
        dic['name']=item['name']
        dic['insertedAtTimestamp']=datetime.now().strftime("%m-%d-%Y %H:%M:%S")
        dic['Address']=item['location']['display_address'][0]
        dic['Rating']=Decimal(str(item['rating']))
        dic["coordLatitude"] = Decimal(str(item["coordinates"]["latitude"]))
        dic["coordLongitude"] = Decimal(str(item['coordinates']['longitude']))
        dic['NumberOfReviews']=item['review_count']
        dic['Zipcode']=item['location']['zip_code']
        dic['Cuisine']=cuisine
        restaurants_set.add(item['alias'])
        items_list.append(dic)
    return

def yelp_scrape(cuisines_list,headers, url, items_list):
    restaurants_set = set()
    for cuisine in cuisines_list:
        for offset in range(0, 1000, 50):
            params = {'term': 'restaurants', 'location':'Manhattan','categories': cuisine, 'offset': offset, 'limit':50}
            response=requests.request('GET',url, params=params, headers=headers)
            if response.status_code != 200:
                print('The status code is {}, please try again.'.format(response.status_code))
                return
            result = json.loads(response.text)['businesses']
            format_data(result, cuisine, items_list, restaurants_set)

def import_data(data, table):
    for item in data:
        table.put_item(Item=item)

def build_es_index(es_list):
    host = "search-restaurants-wfx7vnh776iswobhxqwo3zq7fa.us-east-1.es.amazonaws.com"
    region = 'us-east-1'
    service = "es"
    awsauth = AWS4Auth(access_key, secret_key, region, service)
    es_client = OpenSearch(hosts=[{'host': host, 'port': 443}],
                           http_auth=awsauth, use_ssl=True,
                           verify_certs=True, connection_class=RequestsHttpConnection
                           )
    index_name = 'restaurants'
    index_body = {
        'settings': {
            'index': {
                'number_of_shards': 1
            }
        },
        "mappings": {
            "properties": {
                "id": {
                    "type": "text"
                },
                "cuisine": {
                    "type": "text"
                },
            }
        }
    }
    index_build_response = es_client.indices.create(index_name, body=index_body)
    for item in es_list:
        data_import_response = es_client.index(index="restaurants", body=item)
    return
def main():
    cuisines_list = ['chinese', 'mexican', 'french', 'italian', 'japanese', 'thai', 'mediterranean']
    restaurants_list = []
    yelp_scrape(cuisines_list, headers, url, restaurants_list)
    import_data(restaurants_list, dynamodb_table)
    es_list = []
    for item in restaurants_list:
        id = item["BusinessID"]
        cuisine = item["Cuisine"]
        dic = {"cuisine": cuisine, "id": id}
        es_list.append(dic)
    build_es_index(es_list)

