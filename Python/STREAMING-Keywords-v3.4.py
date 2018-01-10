# -*- coding: utf-8 -*-
'''
Streaming_keywords.py v3.4; release 2018-01-07
@SergeGoussev

This is the main streaming (sample) function but filtered by keywords.
It works with the MySQL_CategorizeNSave v3 script to normalize
the incoming data.
'''
import time, logging, os
import _twitterKeys as twtkeys
from _MySQL_CategorizeNSave3 import Categorize_and_Save

import queue, threading

from tweepy import Stream
from tweepy import OAuthHandler
from tweepy.streaming import StreamListener
#------------------------------------------------------------------------------
#Enter the keys of your twitter applicaiton to connect to the API
consumer_key = '*****'        #consumer key
consumer_secret = '*****'        #consumer secret
access_token = '*****'          #access token
access_token_secret = '*****'    #access secret
#------------------------------------------------------------------------------
sample_type_in_db = "*****"
database_chosen = '*****'

keyword_list = ['*****','*****']

keywords_string = ''
for each in keyword_list:
    keywords_string += each + '; '

saver = Categorize_and_Save(database_chosen=database_chosen,
                            sample_type=sample_type_in_db,
                            keywords_string=keywords_string)

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
                print('Keywords streaming for {} db running,'.format(database_chosen), status.created_at)
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
            except Exception as e:
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
                saver.input_raw_tweet(result)
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
    logger.info('Starting script: {} initializing...'.format(database_chosen))
    q = queue.Queue()

    worker_save = threading.Thread(target=saverfn, name='save-thread', args=(q,))
    worker_stream = threading.Thread(target=streamer, name='stream-thread', args=(q,))

    worker_save.start()
    worker_stream.start()

    q.join()