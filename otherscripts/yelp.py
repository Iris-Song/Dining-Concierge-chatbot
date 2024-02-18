import json
import boto3
import datetime
import requests
from decimal import *
from time import sleep
from elasticsearch import Elasticsearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth

restaurants = set()

def addItems(table, data, location, cuisine):
   global restaurants
   with table.batch_writer() as batch:
        for rec in data:
            item = {}
            itse = {}
            try:
                if rec['alias'] in restaurants:
                    continue
                restaurants.add(rec['alias'])

                #Dynamo
                item['cuisine'] = cuisine
                item['location'] = location
                item['name'] = rec['name']
                item['insertedAtTimestamp'] = str(datetime.datetime.now())
                item["Business ID"] = str(rec["id"])
                item["Rating"] = Decimal(str(rec["rating"]))
                item["Coordinates"] = {}
                item["Coordinates"]["latitude"] = Decimal(str(rec["coordinates"]["latitude"]))
                item["Coordinates"]["longitude"] = Decimal(str(rec["coordinates"]["longitude"]))
                item['Address'] = rec['location']['display_address']
                item['Number of Reviews'] = rec['review_count']
                item['Zip Code'] = rec['location']['zip_code']

                # print(item)
                batch.put_item(Item=item)

                #Elastic
                itse['cuisine'] = cuisine
                itse['RestaurantID'] = str(rec["id"])
                es.index(index="restaurants", doc_type="Restaurant", body=itse)

                sleep(0.001)
            except Exception as e:
                print(e)
                print(rec)
                

if __name__=='__main__':
    API_KEY = 'wHgN8o3nPrvifXQ42SFw6DYzLvkQqfwLdnaSJBuu4MubdJYi5Z1M4n3PS13fKwGEFNAVjPLVR-GWp1FE4MBXxVZYXH57QxYcQ52y47xOf3DykmArU1aQr5Ye7iXRZXYx'
    API='https://api.yelp.com/v3/businesses/search'

    #dynamodb
    client = boto3.resource(service_name='dynamodb')
    table = client.Table('yelp-restaurants')

    #elastic
    credential = boto3.Session(region_name="us-east-1").get_credentials()
    auth = AWS4Auth(credential.access_key, credential.secret_key, 'us-east-1', 'es')
    esEndPoint = 'search-dining-sgk3trcjeib7qhmybkposuxqrm.us-east-1.es.amazonaws.com'

    es = Elasticsearch(
        hosts = [{'host': esEndPoint, 'port': 443}],
        http_auth = auth,
        use_ssl = True,
        verify_certs = True,
        connection_class = RequestsHttpConnection
    )

    locations =  ['manhattan', 'bronx', 'brooklyn', 'queens', 'staten island']
    cuisine = ['chinese','japanese','italian','africa','french']
    headers = {'Authorization': 'Bearer ' + API_KEY}

    for loc in locations:
        for c in cuisine:
            for i in range(0, 1000, 50):
                params = {'location': loc, 'offset': i, 'limit': 50, 'term': c + " restaurants"}
                response = requests.get("https://api.yelp.com/v3/businesses/search", headers = headers, params=params)
                js = response.json()
                # print(js)
                if js.get("businesses", None) is not None:
                    addItems(table, js["businesses"], loc, c)
