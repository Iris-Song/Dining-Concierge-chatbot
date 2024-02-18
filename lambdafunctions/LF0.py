import json
import boto3
import datetime

def lambda_handler(event, context):
    # TODO implement
    # client = boto3.client('lexv2-runtime')
    client = boto3.client('lex-runtime')
   
    # response = client.recognize_text(
    #     botId='ULTWM38SJA',
    #     botAliasId='TSTALIASID',
    #     localeId="en_US",
    #     sessionId="471112677017551",
    #     text= event['messages'][0]['unstructured']['text']
    # )
    response = client.post_text(
        botName='Dining',
        botAlias='prod',
        userId='test',
        inputText= event['messages'][0]['unstructured']['text']
    )
    message = str(response)
    
    if 'message' in response:
        # message = response['messages'][0]['content']
        message = response['message']
        
    botResponse = [{
        'type': 'unstructured',
        'unstructured': {
          "id": "test-id",
          'text': message,
          "timestamp": datetime.datetime.now().strftime("9Y-%m-%d %H:%M:%5")
        }
    }]
      
    return {
        'statusCode': 200,
        'messages': botResponse
    }
    
    
    # responses = [{
    #     "type": "unstructured",
    #     "unstructured": {
    #         "id": "test-id",
    #         "text": "I'm still under development. Please come back later...",
    #         "timestamp": datetime.datetime.now().strftime("9Y-%m-%d %H:%M:%5"),
    #     }
    # }]

    # return {
    #     "statusCode": 200,
    #     "messages": responses,
    # }
        
