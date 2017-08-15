from kafka import KafkaProducer
from httplib import IncompleteRead

import tweepy
import json
import datetime

class TweetListener(tweepy.StreamListener):

    def on_status(self, status):

        """ Callback method for each tweet """
	
        producer.send(topic='tweet', value=status._json)
	print("=> " + str(datetime.datetime.now()))

if __name__ == '__main__':

    #: Setup Kafka Producer
    producer = KafkaProducer(value_serializer=lambda m: json.dumps(m).encode('utf-8'),
                             bootstrap_servers=['ec2-52-39-10-10.us-west-2.compute.amazonaws.com:9092'])

    #: Twitter App Tokens
    CONSUMER_KEY = "wlQggSvm38peEXOzCQxZbyLU8"
    CONSUMER_SECRET = "o8CnsRSh9McOu82CMIVWkWMPc0t4zvX6lMbDIFJ9Sdnix37bcn"
    ACCESS_TOKEN = "59679686-jYSP2b5WKNw742om0XDi9Vf6IhNpx6UqamwF1lnwU"
    ACCESS_TOKEN_SECRET = "TzQn5ygzWyvlHTjdTtj4yJI0GEWdm0oo8ePbzeEgcgIA8"

    #: Twitter API Authentication
    auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
    auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)
    api = tweepy.API(auth)

    #: Activate Listener
    while True:
	try:
    	    listener = TweetListener()
    	    stream = tweepy.Stream(auth = api.auth, listener=listener)
            stream.sample(async=True)
	except (Exception, IncompleteRead), e:
	    print(e)
	    #: In-case of broken pipe, restart the stream 
	    continue
