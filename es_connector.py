from operator import index

from elasticsearch import Elasticsearch, helpers
from pprint import pprint
from nltk.sentiment.vader import SentimentIntensityAnalyzer
import nltk
import os
from elasticsearch import helpers


class EsConnect:
    def __init__(self):
        self.es = Elasticsearch("http://localhost:9200")

    def create_index(self):
        mapping = {
            "properties": {
                "TweetID1": {"type": "float"},
                "CreateDate": {"type": "date",
                 "format": "yyyy-MM-dd HH:mm:ssXXX||EEE MMM dd HH:mm:ss Z yyyy"},
                "Antisemitic": {"type": "integer"},
                "text": {"type": "text"},
                "emotion":{"type":"text"},
                "weapon_detected":{"type":"text"}
            }
        }
        index_name = "tweets"
        self.es.indices.create(index=index_name, body={"mappings": mapping}, ignore=400)

    def delete_index(self):
        self.es.indices.delete(index="tweets", ignore_unavailable=True)

    def insert_df(self,df):
        jsonvalue = df.to_dict(orient='records')
        helpers.bulk(self.es, jsonvalue, index="tweets", chunk_size=1000, request_timeout=200)


    def print_index(self):
        i = 0
        for doc in helpers.scan(self.es, index="tweets", query={"query": {"match_all": {}}}, _source=True):
            pprint(f"Document ID: {doc['_id']}")
            pprint(f"Document Source: {doc['_source']}")
            print("-" * 30)
            i +=1
        print(i)


    def get(self):
        # for doc in helpers.scan(self.es, index="my_documents", query={"query": {"match_all": {}}}, _source=True):
        #     # pprint(f"Document ID: {doc['_id']}")
        #     # pprint(f"Document Source: {doc['_source']}")
        #     # print("-" * 30)
        #     pprint(doc['_source']['text'])
        res = self.es.search(index="tweets", body={"query": {"match_all": {}}},size=10000)
        pprint(len(res['hits']['hits']))

    def _classified_emotion(self, tweet):
        score = SentimentIntensityAnalyzer().polarity_scores(tweet)
        return score["compound"]

    def assign_emotion(self, df):
        nltk_dir = "/tmp/nltk_data"
        os.makedirs(nltk_dir, exist_ok=True)
        nltk.data.path.append(nltk_dir)
        nltk.download('vader_lexicon', download_dir=nltk_dir, quiet=True)  # download vader_lexicon for nltk lib





