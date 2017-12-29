# -*- coding: utf-8 -*-
'''
Streaming_keywords.py v3.3; release 2017-10-30
@SergeGoussev

This is the main streaming (sample) function but filtered by keywords.
It corresponds to the MySQL_CategorizeNSave v2 function to normalize
the incoming data.
'''
import tweepy, time, logging, os
from MySQL_CategorizeNSave2 import CategorizeNsave

import queue, threading

from tweepy import Stream
from tweepy import OAuthHandler
from tweepy.streaming import StreamListener
#------------------------------------------------------------------------------
#Enter the keys of your twitter applicaiton to connect to the API
consumer_key = "****" 				          #consumer key
consumer_secret = "****"			          #consumer secret
access_token = "****"			              #access token
access_token_secret = "****"			      #access secret
#------------------------------------------------------------------------------
sample_name_in_db = 'Stream1'
database_chosen = ''

print("Initiating script...")
username = input("Please input db username: ")
password = input("Please input db password: ")    

#NOTE: you must specify list of keywords below

keyword_list = ['']

keywords_string = ''
for each in keyword_list:
    keywords_string += each + '; '

#NOTE: you must specify the following criteria for the db connection
saver = CategorizeNsave(sample_name=sample_name_in_db, #specify
                        sample_type='STREAMING',
                        keywords_string=keywords_string, #ignore, specified from above
                        username=username, #specify
                        password=password, #specify
                        database_chosen=database_chosen) #specify
#==============================================================================
class StdOutListener(StreamListener):
    '''
    This is an object that collects live tweets pushed by the Twitter Streaming
    API. The stream here is filtered by specified keywords.

    Note:
    This is also a multi-thread object, it operates the save and stream functions
    separately in order to manage workflow on a PC better. If there is a large spike
    of statuses, a queue is created to normalize and save at a later time. This
    minimizes the risk that the Twitter API disconnects the connection due to the
    computer that is used is too slow to categorize and save all incoming statuses.
    '''
        
    def on_status(self, status):
        while True:
            try:
                q.put(status)
                print('Keywords streamer running,', status.created_at)
                break
            except Exception as e:
                if e == 503:
                    print(str(e), ', server unavailible, waiting...')
                    time.sleep(30)
                    continue
                if e == 420:
                    print('420 error, waiting')
                    time.sleep(60)
                    continue
            except:
                print(str(e), type(e), len(e))
                logger.info('\n')
                logger.info("On_status errors: \n{} \n{} \n{}".format(str(e), type(e), len(e)))
                logger.info('\n')
                time.sleep(15)
                continue
        return True

    def on_timeout(self):
        logger.info('\n')
        logger.info('StdOutListener, Timeout... waiting 15s')
        logger.info('\n')
        time.sleep(15)
        return True

    def on_ReadTimeoutError(self):
        logger.info('\n')
        logger.info('StdOutListener, ReadTimeoutError...')
        logger.info('\n')
        return True

    def on_disconnect(self, notice):
        logger.info('\n')
        logger.info('StdOutListener, Disconnect...waiting 30s')
        logger.info('\n')
        time.sleep(30)
        return True

    def on_exception(self, exception):
        logger.info('\n')
        logger.info('StdOutListener, Exception...')
        logger.info('\n')
        return True

    def on_AttributeError(self, error):
        logger.info('\n')
        logger.info('StdOutListener, AttributeError...waiting 15s')
        logger.info('\n')
        time.sleep(15)
        return True

    def on_TypeError(self, error):
        logger.info('\n')
        logger.info('StdOutListener, TypeError...waiting 15s')
        logger.info('\n')
        time.sleep(15)
        return True

    def on_error(self, status):
        logger.info('\n')
        logger.info("StdOutListener, general error: \n{}".format(status))
        logger.info('\n')
        print(status)
        time.sleep(15)
        return True

def saverfn(q):
    print('saver thread running...')
    while True:
        if q.empty() == False:
            result = q.get()
            try:
                saver.raw_input(result)
                print('%s: tweet saved %s'%(worker_save.name, result.created_at))
            except Exception as e:
                logger.info('\n')
                logger.info("Saver f'n crashed, error: \n{}".format(str(e)))
                logger.info('\n')
                continue
        else:
            time.sleep(10)

def streamer(q):
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    while True:
        try:
            stream = Stream(auth, StdOutListener())
            stream.filter(track=keyword_list)
        except (TypeError, AttributeError):
            logger.info('\n')
            logger.info("streamer f'n: exception raised: \nTypeError {}; \nAttributeError {}".format(TypeError, AttributeError))
            logger.info('\n')
            continue
        except:
            stream.disconnect()
            logger.info('\n')
            logger.info("streamer f'n: disconnected... will reconnect in 30")
            logger.info('\n')
            time.sleep(30)
            continue
            
def __logging__():
    #step 1: create logging for errors
    logging.basicConfig(filename='__logs/'+os.path.basename(__file__)[:-2]+'log',
                        level=logging.DEBUG,
                        format='%(asctime)s %(message)s',
                        datefmt='%m/%d/%Y %I:%M:%S %p')
    global logger
    logger = logging.getLogger(__name__)


if __name__ == '__main__':
    __logging__()
    logger.info('\n')
    logger.info('='*20)
    logger.info('Starting script: {} initializing...'.format(sample_name_in_db))
    q = queue.Queue()

    worker_save = threading.Thread(target=saverfn, name='save-thread', args=(q,))
    worker_stream = threading.Thread(target=streamer, name='stream-thread', args=(q,))

    worker_save.start()
    worker_stream.start()

    q.join()