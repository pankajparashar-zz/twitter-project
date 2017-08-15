from kafka import KafkaConsumer

import json
import psycopg2
import time
import datetime

class Tweet:

    def __init__(self, json):

        self.tweet = json

        self.favorite_count = self.get_value('favorite_count')
        self.created_at = self.get_value('created_at')
        self.id = self.get_value('id')
        self.in_reply_to_screen_name = self.get_value('in_reply_to_screen_name')
        self.in_reply_to_user_id_str = self.get_value('in_reply_to_user_id_str')
        self.in_reply_to_status_id_str = self.get_value('in_reply_to_status_id_str')
        self.user_lang = self.get_value('user', 'lang')
        self.place_name = self.get_value('place', 'name')
        self.place_country = self.get_value('place', 'country')
        self.place_full_name = self.get_value('place', 'full_name')
        self.retweeted = self.get_value('retweeted')
        self.retweet_count = self.get_value('retweet_count')
        self.retweeted_status_text = self.get_value('retweeted_status', 'text')
        self.source = self.get_value('source')
        self.text = self.get_value('text')
        self.user_id = self.get_value('user', 'id')
        self.user_screen_name = self.get_value('user', 'screen_name')
        self.user_description = self.get_value('user', 'description')
        self.user_withheld_in_countries = self.get_value('user', 'withheld_in_countries')
        self.favorited = self.get_value('favorited')
        self.truncated = self.get_value('truncated')
        self.entities_hashtags = self.get_multiple_values(self.get_value('entities', 'hashtags'), 'text')
        self.entities_urls = self.get_multiple_values(self.get_value('entities', 'urls'), 'url')

        self.contributors = self.get_multiple_values(self.get_value('contributors'), 'id_str')
        self.latitude, self.longitude = self.get_coordinates(self.get_value('coordinates', 'coordinates'))
        self.place_street_address = self.get_value('place', 'street_address	')
        self.scopes = self.dump(self.get_value('scopes'))
        self.possibly_sensitive = self.get_value('possibly_sensitive')

        symbols = '' #: Complex object
        usrRId = ''  #: Duplicate of user_id?
        umes = '' #: ?
        isRetweet = '' #: Duplicate of retweeted?
        isRetweetedByMe = '' #: ?
        extMediaEntities = '' #: ?
        mediaEntities = '' #: Complex object

    def get_value(self, *keys):

        """ Extract value from nested JSON structure """

        val = None
        tmp = self.tweet

        for key in keys:
            val = tmp.get(key)
            if val:
                tmp = val
            else:
                return val
        if val:
            return val

    @classmethod
    def get_multiple_values(cls, vals, key):

        """ Chain all the vals """

        if vals:
            return ';'.join([val.get(key, '') for val in vals])

    @classmethod
    def get_coordinates(cls, coordinates):

       """ Returns a tuple of latitude and longitude """

       try:
          return coordinates[0], coordinates[1]
       except:
          return None, None

    @classmethod
    def dump(cls, val):

      """ Stringify value """

      if isinstance(val, dict):
          return json.dumps(val)

if __name__ == '__main__':

    BATCH = 1000
    RECORDS = ','.join(['%s'] * BATCH)
    QUERY = """ INSERT INTO livetweets ( contributors, tweetText, usrId, source, screenName, usrDescription, withHeldCountries, isRetweeted, reTwtCount, reTwtdText, favoriteCount, dte, id, inReplyToScreenName, inReplyToUserId, inReplyToStatusId, language, placeName, country, placeFullName, isFavorited, isTruncated, hashTags, urls, latitute, longitude, ispossiblysensitive, scopes, streetaddress) VALUES {0}""".format(RECORDS)

    #: Setup Kafka Consumer
    consumer = KafkaConsumer('tweet',
                             value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                             bootstrap_servers=['ec2-52-39-10-10.us-west-2.compute.amazonaws.com:9092'])
    
    while True:
	BUFFER = []
        try:
    	    #: Setup Redshift Database Connection
    	    con = psycopg2.connect(dbname='twittered',
                           host='twtdb.cdeqw9a0erbr.us-west-2.redshift.amazonaws.com',
                           port='5439',
                           user='pparashar',
                           password='x*8lc5VUNFBY')
    	    cur = con.cursor()

    	    #: Start consuming messages
    	    for message in consumer:
            	tweet = Tweet(json=message.value)
            	attrs = (tweet.contributors, tweet.text, tweet.user_id, tweet.source, tweet.user_screen_name, tweet.user_description, tweet.user_withheld_in_countries, tweet.retweeted, tweet.retweet_count, tweet.retweeted_status_text, tweet.favorite_count, tweet.created_at, tweet.id, tweet.in_reply_to_screen_name, tweet.in_reply_to_user_id_str, tweet.in_reply_to_status_id_str, tweet.user_lang, tweet.place_name, tweet.place_country, tweet.place_full_name, tweet.favorited, tweet.truncated, tweet.entities_hashtags, tweet.entities_urls, tweet.latitude, tweet.longitude, tweet.possibly_sensitive, tweet.scopes, tweet.place_street_address)

            	if len(BUFFER) < BATCH:
               	    BUFFER.append(attrs)
            	else:
                    cur.execute(QUERY, BUFFER)
                    con.commit()
                    BUFFER = [attrs]
		    print('=> ' + str(datetime.datetime.now()))
	except Exception, e:
	    print(str(e))

"""
SAMPLE VALUES
-------------
('contributors', ' => ', None)
('created_at', ' => ', u'Sun Aug 21 11:02:48 +0000 2016')
('entities_hashtags', ' => ', u'MoneyGo')
('entities_urls', ' => ', u'https://t.co/Bq5jw3Xbiv')
('favorite_count', ' => ', 0)
('favorited', ' => ', False)
('id', ' => ', 767316014858014720)
('in_reply_to_screen_name', ' => ', u'mokyu121')
('in_reply_to_status_id_str', ' => ', u'767310679162224640')
('in_reply_to_user_id_str', ' => ', u'701921052008456193')
('latitute', ' => ', 116.76760593)
('longitude', ' => ', -8.97922693)
('place_country', ' => ', u'United Kingdom')
('place_full_name', ' => ', u'Brent, London')
('place_name', ' => ', u'Brent')
('place_street_address', ' => ', None)
('possibly_sensitive', ' => ', False)
('retweet_count', ' => ', 0)
('retweeted', ' => ', False)
('retweeted_status_text', ' => ', None)
('scopes', ' => ', None)
('source', ' => ', u'<a href="http://ifttt.com" rel="nofollow">IFTTT</a>')
('text', ' => ', u'iMagic Trick #app for the iPhone and Apple Watch - Video #applewatch #magic https://t.co/iy4XAKXg03 #iphone \uf8ffWatch')
('truncated', ' => ', False)
('user_description', ' => ', u'iMagicTrick App for the iPhone, iPad, iPod touch and Apple Watch. #iphone #applewatch #ipad #magic #app')
('user_id', ' => ', 3364114641)
('user_lang', ' => ', u'pt')
('user_screen_name', ' => ', u'iMagicTrickApp')
('user_withheld_in_countries', ' => ', None)

SCHEMA
------
                                     Table "public.livetweets"
       Column        |           Type           | Modifiers | Storage  | Stats target | Description
---------------------+--------------------------+-----------+----------+--------------+-------------
 contributors        | character varying(65535) |           | extended |              |
 dte                 | character varying(65535) |           | extended |              |
 usrrid              | bigint                   |           | plain    |              |
 favoritecount       | integer                  |           | plain    |              |
 latitute            | double precision         |           | plain    |              |
 longitude           | double precision         |           | plain    |              |
 id                  | bigint                   |           | plain    |              |
 inreplytoscreenname | character varying(65535) |           | extended |              |
 inreplytouserid     | bigint                   |           | plain    |              |
 inreplytostatusid   | bigint                   |           | plain    |              |
 language            | character varying(65535) |           | extended |              |
 placename           | character varying(65535) |           | extended |              |
 streetaddress       | character varying(65535) |           | extended |              |
 country             | character varying(65535) |           | extended |              |
 placefullname       | character varying(65535) |           | extended |              |
 retwtcount          | integer                  |           | plain    |              |
 retwtdtext          | character varying(65535) |           | extended |              |
 scopes              | character varying(65535) |           | extended |              |
 source              | character varying(65535) |           | extended |              |
 tweettext           | character varying(65535) |           | extended |              |
 usrid               | bigint                   |           | plain    |              |
 screenname          | character varying(65535) |           | extended |              |
 usrdescription      | character varying(65535) |           | extended |              |
 withheldcountries   | character varying(65535) |           | extended |              |
 isfavorited         | boolean                  |           | plain    |              |
 ispossiblysensitive | boolean                  |           | plain    |              |
 isretweet           | boolean                  |           | plain    |              |
 isretweeted         | boolean                  |           | plain    |              |
 isretweetedbyme     | boolean                  |           | plain    |              |
 istruncated         | boolean                  |           | plain    |              |
 extmediaentities    | character varying(65535) |           | extended |              |
 hashtags            | character varying(65535) |           | extended |              |
 mediaentities       | character varying(65535) |           | extended |              |
 symbols             | character varying(65535) |           | extended |              |
 urls                | character varying(65535) |           | extended |              |
 umes                | character varying(65535) |           | extended |              |
"""
