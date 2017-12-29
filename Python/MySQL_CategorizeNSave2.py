'''
MySQL_CategorizeNSave v2.2.1
release date: 2017-10-29
author: @SergeGoussev
'''
import MySQLdb, sys
from dateutil import parser
from datetime import datetime

non_bmp_map = dict.fromkeys(range(0x10000, sys.maxunicode + 1), 0xfffd)

class CategorizeNsave:
    '''
    
    The MySQL_CategorizeNSave script takes input of various kinds and saves the incoming
    twitter stream into the MySQL database.

    Allowed scripts:
    - Streamer: this is the main streamer. It saves incoming stream of tweets and
    normalizes it
    - Friendships: the friendship scrapers collect friendships from various samples, scraping
    the master users network.
    - Sample streamer/searchers: whenever specific samples are required, they can also save
    data using this script.

    Notes:
    - MySQL server handles primary/foreign keys for hashtags and urls
    - Python handles uniqueness of tweets, but inefficiently. After each tweet input, a
    SELECT recreates a python var of tweetids. [NOTE this is not efficient, and might cause
    extra warnings as between the time of SELECT and INSERT querires, another script might
    have inserted the same tweet info. While there are no duplication issues (as primary keys
    eliminate duplication), this should be enhanced later.]
    '''
   
    def __init__(self,
                 sample_name,
                 sample_type,
                 keywords_string,
                 username,
                 password,
                 database_chosen):
        '''
        ------------------
        To initialize, the type of input is required:
         - sample_name = 'Stream1' is the default. This signifies just an unrestricted
         normal stream of tweets
         - if anything else is specified, this will be a new sample table that will be
         created under the specified name.
        '''
        #First, establish a connection to the database in MySQL server
        try:
            self.__connection__(username,
                                password,
                                database_chosen)
        except Exception as e:
            print('Unable to connect,',str(e))
            sys.exit()
        #create table for the sample and insert values into current the sample_summary table
        self.c.execute("SELECT sample_name FROM sample_summary;")
        previous_samples = [each[0] for each in self.c.fetchall()]
        if sample_name not in previous_samples:
            self.c.execute("INSERT INTO sample_summary (sample_name, keywords, date_start, \
                            sample_type) VALUES (%s, %s, %s, %s);",
                           (sample_name,
                            keywords_string,
                            datetime.now(),
                            sample_type))
            self.conn.commit()
        self.sample_name = sample_name
        try:
            self.c.execute("CREATE TABLE IF NOT EXISTS sample_twts_%s (\
                            twtid VARCHAR(18) PRIMARY KEY);"%self.sample_name)
            self.c.execute("CREATE TABLE IF NOT EXISTS sample_users_%s (\
                            userid VARCHAR(18) PRIMARY KEY, \
                            imrev INTEGER, \
                            done VARCHAR(1));"%self.sample_name)
        except Exception as e:
                print('Erred in creating table:',str(e))
        self.conn.commit()
        #Now create variables in memory for existing twtids, userids, hashs, urls
        self.__defaultvars__()

    def __connection__(self,
                       username,
                       password,
                       database_chosen):
        #Specify the necessary connection info to your MySQL server environment
        self.conn = MySQLdb.connect(host="localhost",
                                    user=username,
                                    password=password,
                                    db=database_chosen,
                                    charset="utf8mb4")
        self.c = self.conn.cursor()
        
    def __defaultvars__(self):
        #saved so far...
        self.c.execute("SELECT * FROM sample_twts_%s;"%self.sample_name)
        self.sample_tweets = [each[0] for each in self.c.fetchall()]
        #list of existing twtids
        r = self.c.execute("SELECT twtid FROM twt_Master \
                            UNION \
                            SELECT rttwtid FROM twt_rtMaster;")
        if r != 0:
            self.twtids = [each[0] for each in self.c.fetchall()]
        else:
            self.twtids = []
        #list of existing userids
        r = self.c.execute("SELECT userid FROM users_Master;")
        if r != 0:
            self.userids = [each[0] for each in self.c.fetchall()]
        else:
            self.userids = []

    #======================Raw input==================================================
    def raw_input(self, tweet):
        '''
        This takes the raw tweet input and breaks it into its sub-parts

        As each db table has PRIMARY KEY limits, uniqueness is handled by MySQL server
        '''
        #print('saving ... saved %d tweets so far'%len(self.sample_tweets))
        if tweet.id_str not in self.twtids:
            #----------------------------Quote--------------------
            if tweet.is_quote_status == True and hasattr(tweet, 'quoted_status'):
                self.input_tweet(tweet, 'Normal tweet')
                self.input_tweet(tweet, 'Quoting info')
                if tweet.quoted_status['id_str'] not in self.twtids:
                    self.input_tweet(tweet, 'Quoted tweet')
                if tweet.author.id_str not in self.userids:
                    self.input_user(tweet, 'Tweeting user')
                if tweet.quoted_status['user']['id_str'] not in self.userids and \
                   tweet.quoted_status['user']['id_str'] != tweet.author.id_str:
                    self.input_user(tweet, 'Quoted_user')
            #----------------------------Retweet-------------------
            elif tweet.text[:4] == 'RT @' and hasattr(tweet, 'retweeted_status'):
                self.input_tweet(tweet, 'Retweet info')
                if tweet.retweeted_status.id_str not in self.twtids:
                    self.input_tweet(tweet, 'Retweeted tweet')
                if tweet.retweeted_status.author.id_str not in self.userids and \
                   tweet.retweeted_status.author.id_str != tweet.author.id_str:
                    self.input_user(tweet, 'Retweeted user')
                if tweet.author.id_str not in self.userids:
                    self.input_user(tweet, 'Tweeting user')
            #----------------------------Regular tweet-------------
            else:
                self.input_tweet(tweet, 'Normal tweet')
                if tweet.author.id_str not in self.userids:
                    self.input_user(tweet, 'Tweeting user')
        #now reset the default vars. This guarantees if saving is being done concurently
        #with more than one script, no accidental duplication of ids is made
        self.__defaultvars__()
        
    #===============================================================================
    def input_tweet(self, tweet, ttype):
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
            self.master_tweet(tweet.quoted_status['id_str'],
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
                self.input_tweet_geo(tweet.quoted_status['id_str'],
                                     tweet.quoted_status['geo']['coordinates'][0],
                                     tweet.quoted_status['geo']['coordinates'][0])
        elif ttype == 'Quoting info':
            self.c.execute("INSERT IGNORE INTO twt_qtMaster (twtid, qttwtid) VALUES (%s, %s)",
                              (tweet.id_str,
                               tweet.quoted_status['id_str']))
            self.conn.commit()
        elif ttype == 'Retweeted tweet':
            self.master_tweet(tweet.retweeted_status.id_str,
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
                self.input_tweet_geo(tweet.retweeted_status.id_str,
                                     tweet.retweeted_status.geo['coordinates'][0],
                                     tweet.retweeted_status.geo['coordinates'][1])
        elif ttype == 'Normal tweet':
            self.master_tweet(tweet.id_str,
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
            self.c.execute("INSERT IGNORE INTO twt_rtMaster (twtid, userid, rt_createdat, rttwtid) \
                        VALUES (%s, %s, %s, %s)",
                      (tweet.id_str,
                       tweet.author.id_str,
                       tweet.created_at,
                       tweet.retweeted_status.id_str))
            self.conn.commit()
            self.write_sample_table(tweet.id_str)
        if (ttype == 'Retweet info' or ttype == 'Normal tweet') and tweet.geo is not None:
            self.input_tweet_geo(tweet.id_str,
                                 tweet.geo['coordinates'][0],
                                 tweet.geo['coordinates'][1])


    def input_tweet_geo(self, tweetid, geolat, geolon):
        self.c.execute("INSERT INTO meta_twt_coordinates (twtid, geolat, geolon) VALUES \
                        (%s, %s, %s)",
                       (tweetid,
                        geolat,
                        geolon))
        self.conn.commit()
    
    def input_user(self, tweet, utype):
        '''
        This function takes the raw tweet and a category of the tweet type:
        Options include:
         - 'Quoted user'
         - 'Retweeted user'
         - 'Tweeting user'
        The function then saves the user info based on type of tweet
        '''
        if utype == 'Quoted user':
            self.master_user(tweet.quoted_status['user']['id_str'],
                             tweet.quoted_status['user']['screen_name'],
                             tweet.quoted_status['user']['name'],
                             tweet.quoted_status['user']['description'],
                             tweet.quoted_status['user']['location'],
                             tweet.quoted_status['user']['created_at'],
                             tweet.quoted_status['user']['lang'],
                             tweet.quoted_status['user']['friends_count'],
                             tweet.quoted_status['user']['followers_count'])
        elif utype == 'Retweeted user':
            self.master_user(tweet.retweeted_status.author.id_str,
                             tweet.retweeted_status.author.screen_name,
                             tweet.retweeted_status.author.name,
                             tweet.retweeted_status.author.description,
                             tweet.retweeted_status.author.location,
                             tweet.retweeted_status.author.created_at,
                             tweet.retweeted_status.author.lang,
                             tweet.retweeted_status.author.friends_count,
                             tweet.retweeted_status.author.followers_count)
        elif utype == 'Tweeting user':
            self.master_user(tweet.author.id_str,
                             tweet.author.screen_name,
                             tweet.author.name,
                             tweet.author.description,
                             tweet.author.location,
                             tweet.author.created_at,
                             tweet.author.lang,
                             tweet.author.friends_count,
                             tweet.author.followers_count)
    
    #===============================================================================
    def master_user(self,
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
        self.c.execute("INSERT IGNORE INTO users_Master (userid, username, givenname, acc_createdat, \
                        user_lang) VALUES (%s, %s, %s, %s, %s)",
                       (userid,
                        username,
                        name,
                        createdat,
                        lang))
        self.conn.commit()
        if bio is not None:
            if len(bio) != 0:
                self.c.execute("REPLACE INTO users_bio (userid, bio) VALUES (%s, %s)",
                               (userid,
                                bio))
                self.conn.commit()
        if geo is not None:
            if len(geo) != 0:
                self.c.execute("REPLACE INTO users_geo (userid, geo) VALUES (%s, %s)",
                               (userid,
                                geo))
                self.conn.commit()
        self.c.execute("REPLACE INTO users_numbers (userid, numfriends, numfollowers) \
                        VALUES (%s, %s, %s)",
                       (userid,
                        numfriends,
                        numfollowers))
        self.conn.commit()
        self.c.execute("INSERT IGNORE INTO sample_users_%s (userid) VALUES (%s);"%(self.sample_name,
                                                                                   userid))
        self.conn.commit()
        #------------------------------

    def master_tweet(self,
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
        if '+0000' in str(createdat):
            createdat = parser.parse(str(createdat)).replace(tzinfo=None)
        self.c.execute("INSERT IGNORE INTO twt_Master (twtid, userid, twttext, twt_createdat, \
                        twt_lang) VALUES (%s, %s, %s, %s, %s)",
                       (twtid,
                        userid,
                        text,
                        createdat,
                        lang))
        self.conn.commit()
        self.c.execute("REPLACE INTO twt_numbers (twtid, favcount, rtcount) VALUES \
                        (%s, %s, %s)",
                       (twtid,
                        favcount,
                        rtcount))
        self.conn.commit()
        if len(urls) > 0:
            for url in urls:
                #urls inserted dynamically (duplicates and key generation handled by MySQL server)
                self.c.execute("""
                                INSERT IGNORE INTO meta_urlMaster2 (
                                    url)
                                VALUES ('{}');
                                """.format(str(url['expanded_url'])))
                self.conn.commit()
                self.c.execute("""
                                INSERT IGNORE INTO meta_urlMaster1 (
                                    twtid,
                                    urlid)
                                VALUES ({},
                                        (SELECT urlid FROM meta_urlMaster2 WHERE url='{}'));
                                """.format(twtid, str(url['expanded_url'])))
                self.conn.commit()
        if len(hashs) > 0:
            for hashtag in hashs:
                #hashtag inserted dynamically (duplicates and key generation handled by MySQL server)
                self.c.execute("""
                                INSERT IGNORE INTO meta_hashMaster2 (
                                    hashtag)
                                VALUES ('{}');
                                """.format(hashtag['text'].lower()))
                self.conn.commit()
                self.c.execute("""
                                INSERT IGNORE INTO meta_hashMaster1 (
                                    twtid,
                                    hashid)
                                VALUES ({},
                                        (SELECT hashid FROM meta_hashMaster2 WHERE hashtag='{}'));
                                """.format(twtid, hashtag['text'].lower()))
                self.conn.commit()

        if len(mentions) > 0:
            for mention in mentions:
                self.c.execute("INSERT IGNORE INTO meta_mentionsMaster (twtid, userid) VALUES (%s, %s)",
                          (twtid, mention['id_str']))
                self.conn.commit()
        if inreplyto is not None:
            self.c.execute("INSERT IGNORE INTO meta_repliesMaster (twtid, inreplytotwtid) VALUES (%s, %s)",
                      (twtid, inreplyto))
            self.conn.commit()
        self.write_sample_table(twtid)

    def write_sample_table(self, twtid):
        #finally, write into the samle table the id of the main tweet
        self.c.execute("INSERT IGNORE INTO sample_twts_%s (twtid) VALUES (%s)"%(self.sample_name,
                                                             twtid))
        self.conn.commit()
        #------------------------------ 

    def id_gen(self, input_id):
        outputid = input_id + 1
        return outputid

    def close(self):
        self.db.close