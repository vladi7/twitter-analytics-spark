import sys

import tweepy
from tweepy import OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener
from datetime import datetime
from subprocess import PIPE, Popen
import subprocess

import os


ckey = sys.argv[1]
csecret = sys.argv[2]
atoken = sys.argv[3]
asecret = sys.argv[4]
languageslist =  map(str,  sys.argv[5].strip('[]').split(','))
wordslist =  map(str,  sys.argv[6].strip('[]').split(','))
count = int(sys.argv[7])

consumer_key = ckey
consumer_secret = csecret
access_token = atoken
access_secret = asecret

auth = OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_secret)

api = tweepy.API(auth)


tweets_data_path = '/data/twitter_data.txt'

def run_cmd(args_list):
    """
    run linux commands
    """
    # import subprocess
    print('Running system command: {0}'.format(' '.join(args_list)))
    proc = subprocess.Popen(args_list, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    s_output, s_err = proc.communicate()
    print(s_err)
    s_return =  proc.returncode
    return s_return, s_output, s_err


class MyListener(StreamListener):

    def __init__(self, api=None):
        super(MyListener, self).__init__()
        self.num_tweets = 0
        self.now = datetime.now()
        current_time = self.now.strftime("%H:%M:%S")
        print("Start Time =", current_time)

    def on_data(self, data):
        try:
            with open('tweetsnew1.json', 'a') as f:
                f.write(data)
                self.num_tweets += 1
                if self.num_tweets < count:
                    print(self.num_tweets)
                    return True
                else:
                    now = datetime.now()
                    current_time = now.strftime("%H:%M:%S")
                    print("End time =", current_time)
                    return False
        except BaseException as e:
            print("Error on_data: %s" % str(e))
            return True

    def on_error(self, status):
        print(status)
        print("Incorrect credintials or too many requests")
        sys.exit(1)
        return False


twitter_stream = Stream(auth, MyListener())
twitter_stream.filter(languages=languageslist, track=wordslist)
(ret, out, err)= run_cmd(['/opt/hadoop/bin/hdfs', 'dfs','-moveFromLocal', '-f', os.path.abspath("tweetsnew1.json") , '/'])
