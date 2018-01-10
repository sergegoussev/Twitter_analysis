# -*- coding: utf-8 -*-
'''
_Twitter_Downloader v2.0
release date: 2018-01-07
author: @SergeGoussev
'''
import tweepy, time, sys

class downloader:
    '''
    This object downloads from the twitter API 
    '''
    def __init__(self,
                 consumer_key,
                 consumer_secret,
                 access_token,
                 access_token_secret):
        auth = tweepy.OAuthHandler(consumer_key,
                                   consumer_secret)
        auth.set_access_token(access_token, 
                              access_token_secret)
        self.api = tweepy.API(auth)
    
    def get_tweets(self, chunk_twtids):
        '''
        get_tweets takes a twtids chunk as input (chunk can be any size <=100)
        '''
#        self.__check_list__(chunk_twtids)
        while True:
            try:
                print('checking statuses...')
                statuses = self.api.statuses_lookup(chunk_twtids)
                return statuses
            except tweepy.TweepError as e:
                print(str(e))
                time.sleep(60)
        
    def __check_list__(self, chunk_twtids):
        if (len(chunk_twtids) <= 100 and type(chunk_twtids[0]) is str):
            pass
        else:
            print('Improper input. Please input list of twtids chunked to 100 each')
            sys.exit()
        
if __name__ == '__main__':
    pass
