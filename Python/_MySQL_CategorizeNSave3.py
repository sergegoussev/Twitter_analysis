'''
MySQL_CategorizeNSave v3.0 dev
release date: 2018-01-07
author: @SergeGoussev
'''
import sys
import _DB_connection as DB_con
from dateutil import parser
from datetime import datetime

non_bmp_map = dict.fromkeys(range(0x10000, sys.maxunicode + 1), 0xfffd)

class Categorize_and_Save:
    '''
    The MySQL_CategorizeNSave script takes input of various kinds and saves the incoming
    twitter stream into the MySQL database.

    Allowed scripts:
    - Streamer: this is the main streamer. It saves incoming stream of tweets and
    normalizes it
    - Friendships: the friendship scraper collects friendships for various samples, scraping
    the master users network.
    - Sample streamer/searchers: whenever specific samples are required, they can also save
    data using this script.

    Notes:
    - MySQL server uses primary/foreign keys and handles all redundancy in the data, python 
    does nothing in this case
    '''
    def __init__(self,
                 database_chosen,
                 sample_name='Stream1',
                 sample_type=None,
                 keywords_string=None):
        '''
        ------------------
        To initialize, the type of input is required:
         - sample_name = 'Stream1' is the default. 
         - if anything else is specified, this will be a new sample table that will be
         created under the specified name.
        '''
        #First, establish a connection to the database in MySQL server 
        self.db = DB_con.DB(database_chosen)
        #create table for the sample and insert values into current the sample_summary table
        p_query = "SELECT sample_name FROM sample_summary;"
        previous_samples = [each[0] for each in self.db.query(p_query)]
        if sample_name not in previous_samples:
            query = "INSERT INTO sample_summary (sample_name, keywords, date_start, sample_type) VALUES (%s, %s, %s, %s);"
            values = (sample_name,
                      keywords_string,
                      datetime.now(),
                      sample_type)
            self.db.query(query, values, q_type='INSERT')
        self.sample_name = sample_name
        
        #try to create sample tables if they dont exist
        twts_query = "CREATE TABLE IF NOT EXISTS sample_twts_{} \
            (twtid VARCHAR(18) PRIMARY KEY);".format(self.sample_name)
        users_query = "CREATE TABLE IF NOT EXISTS sample_users_{} \
            (userid VARCHAR(18) PRIMARY KEY, imrev INTEGER, \
            done VARCHAR(1));".format(self.sample_name)
        self.db.query(twts_query, q_type='CREATE')
        self.db.query(users_query, q_type='CREATE')
    
#    #======================Raw input==================================================
    def input_raw_tweet(self, tweet):    
        '''
        This takes the raw tweet input and breaks it into its sub-parts

        As each db table has PRIMARY KEY limits, uniqueness is handled by MySQL server
        '''
        
        #print('saving ... saved %d tweets so far'%len(self.sample_tweets)
        #----------------------------Retweet-------------------
        if hasattr(tweet, 'retweeted_status'):
            if hasattr(tweet.retweeted_status, 'quoted_status'):
                #if this is a retweet of a quoting tweet, capture the original too
                self.__input_tweet__(tweet.retweeted_status, 'Normal tweet')
                self.__input_tweet__(tweet.retweeted_status, 'Quoting info')
                self.__input_tweet__(tweet.retweeted_status, 'Quoted tweet')
                self.__input_twt_user__(tweet.retweeted_status, 'Tweeting user')
                self.__input_twt_user__(tweet.retweeted_status, 'Quoted user')
            self.__input_tweet__(tweet, 'Retweet info')
            self.__input_tweet__(tweet, 'Retweeted tweet')
            self.__input_twt_user__(tweet, 'Tweeting user')
            self.__input_twt_user__(tweet, 'Retweeted user')
            #----------------------------Quote--------------------
        elif hasattr(tweet, 'quoted_status'):
            self.__input_tweet__(tweet, 'Normal tweet')
            self.__input_tweet__(tweet, 'Quoting info')
            self.__input_tweet__(tweet, 'Quoted tweet')
            self.__input_twt_user__(tweet, 'Tweeting user')
            self.__input_twt_user__(tweet, 'Quoted user')
            #----------------------------Regular tweet-------------
        else:
            self.__input_tweet__(tweet, 'Normal tweet')
            self.__input_twt_user__(tweet, 'Tweeting user')
        
    #===============================================================================
    def __input_tweet__(self, tweet, ttype):
        '''
        This function takes the raw tweet and a category of the tweet type:
        Options include:
         - 'Quoted tweet'
         - 'Quoting info'
         - 'Retweeted tweet'
         - 'Normal tweet'
         - 'Retweet info'
        The function then saves the tweet info based on type of tweet
        '''
        if ttype == 'Quoted tweet':
            self.__input_tweet_master__(tweet.quoted_status['id_str'],
                                        tweet.quoted_status['user']['id_str'],
                                        tweet.quoted_status['text'],
                                        tweet.quoted_status['created_at'],
                                        tweet.quoted_status['favorite_count'],
                                        tweet.quoted_status['retweet_count'],
                                        tweet.quoted_status['lang'],
                                        tweet.quoted_status['entities']['urls'],
                                        tweet.quoted_status['entities']['hashtags'],
                                        tweet.quoted_status['entities']['user_mentions'],
                                        tweet.quoted_status['in_reply_to_status_id'])
            if tweet.quoted_status['geo'] is not None:
                self.__input_tweet_geo__(tweet.quoted_status['id_str'],
                                         tweet.quoted_status['geo']['coordinates'][0],
                                         tweet.quoted_status['geo']['coordinates'][1])
        elif ttype == 'Quoting info':
            q_info_query = "INSERT IGNORE INTO twt_qtMaster (twtid, qttwtid) VALUES (%s, %s);"
            q_info_values = (tweet.id_str,
                             tweet.quoted_status['id_str'])
            self.db.query(q_info_query, q_info_values, q_type='INSERT')
        elif ttype == 'Retweeted tweet':
            self.__input_tweet_master__(tweet.retweeted_status.id_str,
                                        tweet.retweeted_status.author.id_str,
                                        tweet.retweeted_status.text,
                                        tweet.retweeted_status.created_at,
                                        tweet.retweeted_status.favorite_count,
                                        tweet.retweeted_status.retweet_count,
                                        tweet.retweeted_status.lang,
                                        tweet.retweeted_status.entities['urls'],
                                        tweet.retweeted_status.entities['hashtags'],
                                        tweet.retweeted_status.entities['user_mentions'],
                                        tweet.retweeted_status.in_reply_to_status_id)
            if tweet.retweeted_status.geo is not None:
                self.__input_tweet_geo__(tweet.retweeted_status.id_str,
                                         tweet.retweeted_status.geo['coordinates'][0],
                                         tweet.retweeted_status.geo['coordinates'][1])
        elif ttype == 'Normal tweet':
            self.__input_tweet_master__(tweet.id_str,
                                        tweet.author.id_str,
                                        tweet.text,
                                        tweet.created_at,
                                        tweet.favorite_count,
                                        tweet.retweet_count,
                                        tweet.lang,
                                        tweet.entities['urls'],
                                        tweet.entities['hashtags'],
                                        tweet.entities['user_mentions'],
                                        tweet.in_reply_to_status_id)
        elif ttype == 'Retweet info':
            r_info_query = "INSERT IGNORE INTO twt_rtMaster (twtid, userid,\
                rt_createdat, rttwtid) VALUES (%s, %s, %s, %s);"
            r_info_values = (tweet.id_str,
                             tweet.author.id_str,
                             tweet.created_at,
                             tweet.retweeted_status.id_str)
            self.db.query(r_info_query, r_info_values, q_type='INSERT')
            self.__write_sample_table__(tweet.id_str)
        #geo location of normal tweets or retweets (quotes handled above)
        if (ttype == 'Retweet info' or ttype == 'Normal tweet') and tweet.geo is not None:
            self.__input_tweet_geo__(tweet.id_str,
                                     tweet.geo['coordinates'][0],
                                     tweet.geo['coordinates'][1])

    
    def __input_twt_user__(self, tweet, utype):
        '''
        This function takes the raw tweet and a category of the tweet type:
        Options include:
         - 'Quoted user'
         - 'Retweeted user'
         - 'Tweeting user'
        The function then saves the user info based on type of tweet
        '''
        if utype == 'Quoted user':
            self.__input_user_master__(tweet.quoted_status['user']['id_str'],
                                       tweet.quoted_status['user']['screen_name'],
                                       tweet.quoted_status['user']['name'],
                                       tweet.quoted_status['user']['description'],
                                       tweet.quoted_status['user']['location'],
                                       tweet.quoted_status['user']['created_at'],
                                       tweet.quoted_status['user']['lang'],
                                       tweet.quoted_status['user']['friends_count'],
                                       tweet.quoted_status['user']['followers_count'])
        elif utype == 'Retweeted user':
            self.__input_user_master__(tweet.retweeted_status.author.id_str,
                                       tweet.retweeted_status.author.screen_name,
                                       tweet.retweeted_status.author.name,
                                       tweet.retweeted_status.author.description,
                                       tweet.retweeted_status.author.location,
                                       tweet.retweeted_status.author.created_at,
                                       tweet.retweeted_status.author.lang,
                                       tweet.retweeted_status.author.friends_count,
                                       tweet.retweeted_status.author.followers_count)
        elif utype == 'Tweeting user':
            self.__input_user_master__(tweet.author.id_str,
                                       tweet.author.screen_name,
                                       tweet.author.name,
                                       tweet.author.description,
                                       tweet.author.location,
                                       tweet.author.created_at,
                                       tweet.author.lang,
                                       tweet.author.friends_count,
                                       tweet.author.followers_count)
    
    #===============================================================================
    def __input_user_master__(self,
                              userid,
                              username,
                              name,
                              bio,
                              geo,
                              createdat,
                              lang,
                              numfriends,
                              numfollowers):
        #------------------------------
        #insert ignore user
        user_query = "INSERT IGNORE INTO users_Master (userid, username, givenname, \
        	acc_createdat, user_lang) VALUES (%s, %s, %s, %s, %s);"
        user_values = (userid,
                       username,
                       name,
                       createdat,
                       lang)
        self.db.query(user_query, user_values, q_type='INSERT')
        
        #replace bio (if present)
        if bio is not None:
            if len(bio) != 0:
                user_bio_query = "REPLACE INTO users_bio (userid, bio) VALUES (%s, %s);"
                user_bio_values = (userid, bio)
                self.db.query(user_bio_query,user_bio_values,q_type='REPLACE')

        #replace geo location (if present)
        if geo is not None:
            if len(geo) != 0:
                user_geo_query = "REPLACE INTO users_geo (userid, geo) VALUES (%s, %s);"
                user_geo_values = (userid, geo)
                self.db.query(user_geo_query, user_geo_values, q_type='REPLACE')
        
        #replace user numbers
        user_nums_query = "REPLACE INTO users_numbers (userid, numfriends, \
            numfollowers) VALUES (%s, %s, %s);"
        user_nums_values = (userid,
                            numfriends,
                            numfollowers)
        self.db.query(user_nums_query, user_nums_values, q_type='REPLACE')
        
        #insert ignore sample_user
        samp_users_query = "INSERT IGNORE INTO sample_users_{} \
        	(userid) VALUES (%s);".format(self.sample_name)
        samp_users_values = (userid,)
        self.db.query(samp_users_query, samp_users_values, q_type='INSERT')
        #------------------------------

    def __input_tweet_master__(self,
                               twtid,
                               userid,
                               text,
                               createdat,
                               favcount,
                               rtcount,
                               lang,
                               urls,
                               hashs,
                               mentions,
                               inreplyto):
        #------------------------------
        #deal with tweet creation timestamp sometimes having a timezone of UTC
        #specify and sometimes not
        if '+0000' in str(createdat):
            createdat = parser.parse(str(createdat)).replace(tzinfo=None)
        
        #insert ignore tweet
        twt_query = "INSERT IGNORE INTO twt_Master (twtid, userid, twttext, \
        	twt_createdat, twt_lang) VALUES (%s, %s, %s, %s, %s);"
        twt_values = (twtid,
                      userid,
                      text,
                      createdat,
                      lang)
        self.db.query(twt_query, twt_values, q_type='INSERT')

        #replace tweet numbers
        twt_num_query = "REPLACE INTO twt_numbers (twtid, favcount, rtcount) VALUES \
                        (%s, %s, %s)"
        twt_num_values = (twtid,
                          favcount,
                          rtcount)
        self.db.query(twt_num_query, twt_num_values, q_type='REPLACE')
        
        #insert urls (in a none normalized fashion) 
        #first check if there are urls to save 
        if len(urls) > 0:
            #now, to deal with duplication, first check if twtid exists in db
            url_check_query = "SELECT twtid FROM meta_urlMaster WHERE twtid=%s;"
            url_check_values = (twtid,)
            if len(self.db.query(url_check_query, url_check_values, q_type='SELECT')) is 0:
                #now save each url as its a new tweet
                for url in urls:
                    url_query = "INSERT INTO meta_urlMaster (twtid, url) VALUES (%s, %s);"
                    url_values = (twtid, str(url['expanded_url']))
                    self.db.query(url_query, url_values, q_type='INSERT')
                    
        if len(hashs) > 0:
            for hashtag in hashs:
                #hashtag inserted dynamically (duplicates and key generation handled by MySQL server)
                hash2_query = "INSERT IGNORE INTO meta_hashMaster2 (hashtag) \
                	VALUES (%s);"
                hash2_values = (hashtag['text'].lower(),)
                self.db.query(hash2_query, hash2_values, q_type='INSERT')
                hash1_query = """
                                INSERT IGNORE INTO meta_hashMaster1 (
                                    twtid,
                                    hashid)
                                VALUES (%s,
                                        (SELECT hashid 
                                        FROM meta_hashMaster2 
                                        WHERE hashtag=%s));
                                """
                hash1_values = (twtid,
                	hashtag['text'].lower())
                self.db.query(hash1_query, hash1_values, q_type='INSERT')

        if len(mentions) > 0:
            for mention in mentions:
                mentions_query = "INSERT IGNORE INTO meta_mentionsMaster (twtid, userid) VALUES (%s, %s);"
                mentions_values = (twtid, mention['id_str'])
                self.db.query(mentions_query, mentions_values, q_type='INSERT')
        if inreplyto is not None:
            reply_query = "INSERT IGNORE INTO meta_repliesMaster (twtid, inreplytotwtid) VALUES (%s, %s);"
            reply_values = (twtid, inreplyto)
            self.db.query(reply_query, reply_values, q_type='INSERT')
        #finally, write into the samle table the id of the main tweet
        self.__write_sample_table__(twtid)

    def __write_sample_table__(self, twtid): 
        #finally, write into the samle table the id of the main tweet
        sample_twt_query = "INSERT IGNORE INTO sample_twts_{} (twtid) VALUES \
        	(%s);".format(self.sample_name)
        sample_twt_values = (twtid,)
        self.db.query(sample_twt_query, sample_twt_values, q_type='INSERT')

    def __input_tweet_geo__(self, 
                            tweetid, 
                            geolat, 
                            geolon):
        norm_twt_geo_query = "INSERT INTO meta_twt_coordinates (twtid, geolat, \
            geolon) VALUES (%s, %s, %s);"
        norm_twt_geo_values = (tweetid,
                               geolat,
                               geolon)
        self.db.query(norm_twt_geo_query, norm_twt_geo_values, q_type='INSERT')
        
        
if __name__ == '__main__':
    pass