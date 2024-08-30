"""
 This code sample demonstrates an implementation of the Lex Code Hook Interface
 in order to serve a bot which manages dentist appointments.
 Bot, Intent, and Slot models which are compatible with this sample can be found in the Lex Console
 as part of the 'MakeAppointment' template.

 For instructions on how to set up and test this bot, as well as additional samples,
 visit the Lex Getting Started documentation http://docs.aws.amazon.com/lex/latest/dg/getting-started.html.
"""

import json
import dateutil.parser
import datetime
import time
import os
import math
import logging
import re
import boto3

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)


""" --- Helpers to build responses which match the structure of the necessary dialog actions --- """


def elicit_slot(session_attributes, intent_name, slots, slot_to_elicit, message):
    return {
        'sessionAttributes': session_attributes,
        'dialogAction': {
            'type': 'ElicitSlot',
            'intentName': intent_name,
            'slots': slots,
            'slotToElicit': slot_to_elicit,
            'message': message
        }
    }


def confirm_intent(session_attributes, intent_name, slots, message, response_card):
    return {
        'sessionAttributes': session_attributes,
        'dialogAction': {
            'type': 'ConfirmIntent',
            'intentName': intent_name,
            'slots': slots,
            'message': message,
            'responseCard': response_card
        }
    }


def close(session_attributes, fulfillment_state, message):
    response = {
        'sessionAttributes': session_attributes,
        'dialogAction': {
            'type': 'Close',
            'fulfillmentState': fulfillment_state,
            'message': message
        }
    }

    return response


def delegate(session_attributes, slots):
    return {
        'sessionAttributes': session_attributes,
        'dialogAction': {
            'type': 'Delegate',
            'slots': slots
        }
    }



""" --- Helper Functions --- """


def parse_int(n):
    try:
        return int(n)
    except ValueError:
        return float('nan')


def isvalid_date(date):
    try:
        dateutil.parser.parse(date)
        return True
    except ValueError:
        return False


def build_validation_result(is_valid, violated_slot, message_content):
    return {
        'isValid': is_valid,
        'violatedSlot': violated_slot,
        'message': {'contentType': 'PlainText', 'content': message_content}
    }


def validate_book_appointment(location, cuisine, date, dining_time, people, email):
    location_type = ['manhattan', 'bronx', 'brooklyn', 'queens', 'staten island']
    if location and location.lower() not in location_type:
        return build_validation_result(False, 'Location', 'I did not recognize that, would you like a different location in NYC? \
                                       Our most popular location is Manhattan')
                                       
    cuisine_type = ['chinese','japanese','italian','africa','french']
    if cuisine and cuisine.lower() not in cuisine_type:
        return build_validation_result(False, 'Cuisine', 'I did not recognize that, would you like a different cuisine? \
                                       Our most popular cuisine is Italian')
                                       
    if dining_time:
        if len(dining_time) != 5:
            return build_validation_result(False, 'Time', 'I did not recognize that, what time would you like to dine?')

        hour, minute = dining_time.split(':')
        hour = parse_int(hour)
        minute = parse_int(minute)
        if math.isnan(hour) or math.isnan(minute):
            return build_validation_result(False, 'Time', 'I did not recognize that, what time would you like to dine?')

    if date:
        if not isvalid_date(date):
            return build_validation_result(False, 'Date', 'I did not understand that, what date works best for you?')
        elif datetime.datetime.strptime(date, '%Y-%m-%d').date() < datetime.date.today():
            return build_validation_result(False, 'Date', 'Sorry, I can only give you suggestions after today. Can you try a different date?')
    
    if people:
        people = parse_int(people)
        if math.isnan(people):
            return build_validation_result(False, 'People', 'I did not recognize that, how many people do you have?')
        if people<=0:
            return build_validation_result(False, 'People', 'People number should be greater than 0. Can you try a different number?')
    
    if email:
        if not re.match("^.+\\@(\\[?)[a-zA-Z0-9\\-\\.]+\\.([a-zA-Z]{2,3}|[0-9]{1,3})(\\]?)$", email):
            return build_validation_result(False, 'Email', 'The email address is invalid. Can you try a different email?')

    return build_validation_result(True, None, None)


""" --- Functions that control the bot's behavior --- """


def make_appointment(intent_request):
    """
    Performs dialog management and fulfillment for booking a dentists appointment.

    Beyond fulfillment, the implementation for this intent demonstrates the following:
    1) Use of elicitSlot in slot validation and re-prompting
    2) Use of confirmIntent to support the confirmation of inferred slot values, when confirmation is required
    on the bot model and the inferred slot values fully specify the intent.
    """
    
    location = intent_request['currentIntent']['slots']["Location"]
    cuisine = intent_request['currentIntent']['slots']["Cuisine"]
    date = intent_request['currentIntent']['slots']["Date"]
    dining_time = intent_request['currentIntent']['slots']["Time"]
    people = intent_request['currentIntent']['slots']["People"]
    email = intent_request['currentIntent']['slots']["Email"]
    source = intent_request['invocationSource']
    output_session_attributes = intent_request['sessionAttributes'] if intent_request['sessionAttributes'] is not None else {}
    
    

    if source == 'DialogCodeHook':
        # Perform basic validation on the supplied input slots.
        slots = intent_request['currentIntent']['slots']
        validation_result = validate_book_appointment(location, cuisine, date, dining_time, people, email)
        if not validation_result['isValid']:
            slots[validation_result['violatedSlot']] = None
            return elicit_slot(
                output_session_attributes,
                intent_request['currentIntent']['name'],
                slots,
                validation_result['violatedSlot'],
                validation_result['message']
            )

        return delegate(output_session_attributes, slots)
     
    messageBody = {"Location": location, "Cuisine": cuisine, "Date": date, \
                  "Time": dining_time, "People": people, "Email": email}

    messageBody = json.dumps(messageBody)

    sqs = boto3.client('sqs')
    
    sqs.send_message(
        QueueUrl="https://sqs.us-east-1.amazonaws.com/xxxxxx/Q1",
        MessageBody=messageBody
    )
    
    return close(
        output_session_attributes,
        'Fulfilled',
        {
            'contentType': 'PlainText',
            'content': 'Okay, I will notify you via email when we have the recommendations ready.'
        }
    )


""" --- Intents --- """


def dispatch(intent_request):
    """
    Called when the user specifies an intent for this bot.
    """

    logger.debug('dispatch userId={}, intentName={}'.format(intent_request['userId'], intent_request['currentIntent']['name']))

    intent_name = intent_request['currentIntent']['name']

    # Dispatch to your bot's intent handlers
    if intent_name == 'MakeAppointment':
        return make_appointment(intent_request)
    raise Exception('Intent with name ' + intent_name + ' not supported')


""" --- Main handler --- """


def lambda_handler(event, context):
    """
    Route the incoming request based on intent.
    The JSON body of the request is provided in the event slot.
    """
    # By default, treat the user request as coming from the America/New_York time zone.
    os.environ['TZ'] = 'America/New_York'
    time.tzset()
    logger.debug('event.bot.name={}'.format(event['bot']['name']))

    return dispatch(event)
