import sys

import tweepy
from tweepy import OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener
from datetime import datetime
from subprocess import PIPE, Popen
import subprocess

import os
consumer_key = ''
consumer_secret = ''
access_token = ''
access_secret = ''

auth = OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_secret)

api = tweepy.API(auth)

count = 0

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
            with open('tweets_small4.json', 'a') as f:
                f.write(data)
                self.num_tweets += 1
                if self.num_tweets < 100:
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
        return True


twitter_stream = Stream(auth, MyListener())
twitter_stream.filter(languages=["en"], track=["I", "a", "the", "love", "thank", "happy", "great"])
(ret, out, err)= run_cmd(['/opt/hadoop/bin/hdfs', 'dfs', '-copyFromLocal', os.path.abspath("tweets_small4.json") , '/'])
