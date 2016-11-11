from pytz import timezone
from dateutil import parser
from datetime import datetime
from elasticsearch import Elasticsearch
from twitter import Twitter, TwitterStream, OAuth
from threading import Timer, get_ident

OAUTH_INFO = dict(
    token="",
    token_secret="",
    consumer_key="",
    consumer_secret="")

STREAM_INFO = dict(
    timeout=600,
    block=False,
    heartbeat_timeout=600) 

JST = timezone('Asia/Tokyo')
WOEID_JP = 23424856 

class TwitterTrendStream():

    def __init__(self):
        self.__current_thread_ident = None
        self.__oauth = OAuth(**OAUTH_INFO)
        self.__es = Elasticsearch()

    def __fetch_trands(self, twitter):
        response = twitter.trends.place(_id=WOEID_JP)
        return [trend["name"] for trend in response[0]["trends"]]

    def __fetch_filter_stream(self, twitter_stream, track_list):
        track = ",".join(track_list)
        return twitter_stream.statuses.filter(track=track)

    def run(self):
        self.__current_thread_ident = get_ident() 
        Timer(300, self.run).start()              

        twitter = Twitter(auth=self.__oauth)
        twitter_stream = TwitterStream(auth=self.__oauth, **STREAM_INFO)

        trend_list = self.__fetch_trands(twitter)
        tweet_iter = self.__fetch_filter_stream(twitter_stream, trend_list)
		
        for tweet in tweet_iter:
            if "limit" in tweet: 
                continue

            if self.__current_thread_ident != get_ident(): 
                return True
            for trend in trend_list:
                if trend in tweet['text']:
                    doc = {
                        'track': trend,
                        'text': tweet['text'],
                        'created_at': str(parser.parse(tweet['created_at']).astimezone(JST).isoformat())
                    }
                    self.__es.index(index="twi_index", doc_type='twi_type', body=doc)

if __name__ == '__main__':
    TwitterTrendStream().run()
