# -*- coding: utf-8 -*-
"""
Created on Fri Apr 07 03:38:56 2017

@author: Vignesh
"""
#importing required packages
import time #time library to create timeout feature
from tweepy import Stream
from tweepy import OAuthHandler
from tweepy.streaming import StreamListener
import json
from pymongo import MongoClient

#Setting consumer keys and access keys

ckey = 'QawQDeRrRV5R1kD7f79bE9Wqw'
consumer_secret = '7TGSIDXUXg6pKHUQFsBiXL7GaDuFnDMv7mp74lluc6JI9Az7a9'
access_token_key = '809641593867304960-24p7exqrJIPTlSRQxjugQy7oZ6vHcQ6'
access_token_secret = '7ZGb0Q6QjY8xAQrrtm0yStygp44GxPdMNHYwI3I7CMuJ7'


start_time = time.time()
keyword_list = ['sachin']

class listener(StreamListener):
    def __init__(self, start_time, time_limit = 60):
        self.time = start_time
        self.limit = time_limit
    def on_data(self,data):
        while (time.time() - self.time < self.limit):
            try:
                client = MongoClient('localhost',27017)
                db = client['twitter_db']
                collection = db['twitter_sachin_collection']
                tweet = json.loads(data)
                collection.insert(tweet)
                return True
            except BaseException, e:
                print 'failed to load data', str(e)
                time.sleep(5)
                pass
        exit()
    def on_error(self,status):
        print status

        


auth = OAuthHandler(ckey, consumer_secret) #OAuth object
auth.set_access_token(access_token_key, access_token_secret)
 
 
twitterStream = Stream(auth, listener(start_time, time_limit=20)) #initialize Stream object with a time out limit
twitterStream.filter(track=keyword_list, languages=['en'])  #call the filter method to run the Stream Object

#extracting stored tweets from MongoDB
        
client = MongoClient('localhost',27017)
db = client['twitter_db']
collection = db['twitter_collection']
tweets_collected = collection.find()
for tweet in tweets_collected:
    print tweet['text']