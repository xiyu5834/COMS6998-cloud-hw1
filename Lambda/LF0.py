import boto3
# Define the client to interact with Lex
client = boto3.client('lex-runtime')
def lambda_handler(event, context):
  #  msg_from_user = event['messages'][0]
    print(event)
    last_user_message = event["messages"][0]['unstructured']['text'] 
    # change this to the message that user submits on 
    # your website using the 'event' variable
    print(f"Message from frontend: {last_user_message}")
    response = client.post_text(botName='DiningConcierge',
                                botAlias='dine',
                                userId='121',
                               
                                sessionAttributes={},
                                inputText=last_user_message)
    print(response)
    
    msg_from_lex = response['message']
    if msg_from_lex:
        print(f"Message from Chatbot: {msg_from_lex}")
        print(response)
       
        resp =  {
              "messages": [
                {
                  "type": "unstructured",
                  "unstructured": {
                    "id": "string",
                    "text": response['message'],
                    "timestamp": "string"
                  }
                }
              ]
            }
        # modify resp to send back the next question Lex would ask from the user
        
        # format resp in a way that is understood by the frontend
        # HINT: refer to function insertMessage() in chat.js that you uploaded
        # to the S3 bucket
        return resp