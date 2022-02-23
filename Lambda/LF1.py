import math
import dateutil.parser
import datetime
import time
import os
import logging
import boto3
import json

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)


""" --- Helpers to build responses which match the structure of the necessary dialog actions --- """


def get_slots(intent_request):
    return intent_request['currentIntent']['slots']


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


def build_validation_result(is_valid, violated_slot, message_content):
    if message_content is None:
        return {
            "isValid": is_valid,
            "violatedSlot": violated_slot,
        }

    return {
        'isValid': is_valid,
        'violatedSlot': violated_slot,
        'message': {'contentType': 'PlainText', 'content': message_content}
    }


def isvalid_date(date):
    try:
        dateutil.parser.parse(date)
        return True
    except ValueError:
        return False


def validate_dining_info(location, cuisine, people, date, time, phone,email):
    if location is not None and location.lower()!='manhattan':
        return build_validation_result(False,
                                       'Location',
                                       'Sorry, we currently only support search in Manhattan, New York.')
    
    cuisines = ['chinese', 'mexican', 'french', 'italian', 'japanese', 'thai', 'mediterranean']
    if cuisine is not None and cuisine.lower() not in cuisines:
        return build_validation_result(False,
                                       'Cuisine',
                                       'Please select a cuisine from Chinese, French, Japanese, Italian, Mexican, mediterranean, and Thai.')

    if date is not None:
        if not isvalid_date(date):
            return build_validation_result(False, 'Date', 'I did not understand that, what date would you like to go?')
        elif datetime.datetime.strptime(date, '%Y-%m-%d').date() < datetime.date.today():
            return build_validation_result(False, 'Date', 'You can lookup restaurants from today onwards.  What day would you like to go?')

    if people is not None:
        if not people.isnumeric():
            return build_validation_result(False, 'People','Please enter a valid number.')
            
    if email is not None:
        name,company=email.split('@')
        if name==''or company=='':
            return generate_check_result(False, 'Email', 'Please enter your email in its correct format:***@***')
       
            
    if time is not None:
        if len(time) != 5:
            # Not a valid time; use a prompt defined on the build-time model.
            return build_validation_result(False, 'Time', None)

        hour, minute = time.split(':')
        hour = parse_int(hour)
        minute = parse_int(minute)
        if math.isnan(hour) or math.isnan(minute):
            # Not a valid time; use a prompt defined on the build-time model.
            return build_validation_result(False, 'Time', 'Please enter the time in its correct format: xx:xx')

        if hour < 12 or hour > 24:
            # Outside of business hours
            return build_validation_result(False, 'Time', 'Out of business hours for restaurants!')
            
    if phone is not None:
        if not(len(phone) == 10 and phone.isnumeric()):
            phonesplit = phone.split('-')
            if len(phonesplit)==3:
                if not(len(phonesplit[0]) == 3 and phonesplit[0].isnumeric() and len(phonesplit[1]) == 3 and phonesplit[1].isnumeric() and len(phonesplit[2]) == 4 and phonesplit[2].isnumeric()):
                    return build_validation_result(False, 'Phone', 'Please enter the phone number in its correct format. (US number only, no need to add the country code)')
            else:
                return build_validation_result(False, 'Phone', 'Please enter the phone number in its correct format. (US number only, no need to add the country code)')
                

    return build_validation_result(True, None, None)


""" --- Functions that control the bot's behavior --- """

def sqs_send(requestData):
    sqs = boto3.client('sqs')
    queue_url = 'https://sqs.us-east-1.amazonaws.com/292582700758/testQueue'
    delaySeconds = 3
    messageAttributes = {
        'location': {
            'DataType': 'String',
            'StringValue': requestData['location']
        },
        'cuisine': {
            'DataType': 'String',
            'StringValue': requestData['cuisine']
        },
        'time': {
            'DataType': "String",
            'StringValue': requestData['time']
        },
        'date': {
            'DataType': "String",
            'StringValue': requestData['date']
        },
        'number': {
            'DataType': 'Number',
            'StringValue': requestData['number']
        },
        'phone': {
            'DataType' : 'String',
            'StringValue' : requestData['phone']
        }
        ,
        'email': {
            'DataType' : 'String',
            'StringValue' : requestData['email']
        }
    }
    messageBody=('Customer Reservation Suggestions')
    
    response = sqs.send_message(
        QueueUrl = queue_url,
        DelaySeconds = delaySeconds,
        MessageAttributes = messageAttributes,
        MessageBody = messageBody
        )

    print("response", response)
    
    print ('send data to queue')
    print(response['MessageId'])
    
    return response['MessageId']
def dining_suggestions(intent_request):
    """
    Performs dialog management and fulfillment for ordering flowers.
    Beyond fulfillment, the implementation of this intent demonstrates the use of the elicitSlot dialog action
    in slot validation and re-prompting.
    """

    people = get_slots(intent_request)["People"]
    phone = get_slots(intent_request)["Phone"]
    location = get_slots(intent_request)["Location"]
    cuisine = get_slots(intent_request)["Cuisine"]
    date = get_slots(intent_request)["Date"]
    time = get_slots(intent_request)["Time"]
    email = get_slots(intent_request)['Email']
    source = intent_request['invocationSource']
    print(intent_request)
   
    if source == 'DialogCodeHook':
        # Perform basic validation on the supplied input slots.
        # Use the elicitSlot dialog action to re-prompt for the first violation detected.
        slots = get_slots(intent_request)

        validation_result = validate_dining_info(location, cuisine, people, date, time, phone,email)
        print(intent_request)
        if not validation_result['isValid']:
            slots[validation_result['violatedSlot']] = None
            
            return elicit_slot(intent_request['sessionAttributes'],
                               intent_request['currentIntent']['name'],
                               slots,
                               validation_result['violatedSlot'],
                               validation_result['message'])

        # Pass the price of the flowers back through session attributes to be used in various prompts defined
        # on the bot model.
        output_session_attributes = intent_request['sessionAttributes'] if intent_request['sessionAttributes'] is not None else {}
    
        return delegate(output_session_attributes, get_slots(intent_request))

    # Order the flowers, and rely on the goodbye message of the bot to define the message to the end user.
    # In a real bot, this would likely involve a call to a backend service.
    gathered_data = {
        'location': location,
        'cuisine': cuisine,
        'number': people,
        'date': date,
        'time': time,
        'phone': phone,
        'email' : email
    }
    message_ID = sqs_send(gathered_data)
    return close(intent_request['sessionAttributes'],
                 'Fulfilled',
                 {'contentType': 'PlainText',
                  'content': 'You’re all set. Expect my suggestions shortly! Have a good day.'})
""" --- Intents --- """


def dispatch(intent_request):
    """
    Called when the user specifies an intent for this bot.
    """
    logger.debug('dispatch userId={}, intentName={}, source={}'.format(intent_request['userId'], intent_request['currentIntent']['name'], intent_request['invocationSource']))
    intent_name=intent_request['currentIntent']['name']

    if intent_name == 'DiningSuggestionsIntent':
        return dining_suggestions(intent_request)
    elif intent_name == 'ThankYouIntent':
        return thank_you(intent_request)
    elif intent_name == 'GreetingIntent':
        return greeting(intent_request)


    raise Exception('Intent with name ' + intent_name + ' not supported')

def thank_you(intent_request):
    output_session_attributes = intent_request['sessionAttributes'] if intent_request['sessionAttributes'] is not None else {}
    return close(
        output_session_attributes,
        'Fulfilled',
        {
            'contentType': 'PlainText',
            'content': "You are welcome."
        }
    )
    
def greeting(intent_request):
    output_session_attributes = intent_request['sessionAttributes'] if intent_request['sessionAttributes'] is not None else {}
    return close(
        output_session_attributes,
        'Fulfilled',
        {
            'contentType': 'PlainText',
            'content': "Hi there, how can I help?"
        }
    )
    
""" --- Main handler --- """


def lambda_handler(event, context):
    """
    Route the incoming request based on intent.
    The JSON body of the request is provided in the event slot.
    """
    # By default, treat the user request as coming from the America/New_York time zone.
    print(event['bot']['name'])
    os.environ['TZ'] = 'America/New_York'
    time.tzset()
    logger.debug('event.bot.name={}'.format(event['bot']['name']))


    return dispatch(event)