from importlib.util import source_hash
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

    def get_one(self):
        return self.es.get(index="tweets", id="28S-FJkB2QXCJWfbW9w0")

    def get_texts(self):
        res = self.es.search(index="tweets", size=10000 , body=
        {
            "query":
                {
                "match_all": {}
                },
            "_source": "text"
        })
        return res["hits"]["hits"]

    def _classified_emotion(self, tweet):
        score = SentimentIntensityAnalyzer().polarity_scores(tweet)
        if score["compound"] < -0.5:
            return "negative"
        elif score["compound"] < 0.5:
            return "neutral"
        return "positive"


    def init_nltk(self, df):
        nltk_dir = "/tmp/nltk_data"
        os.makedirs(nltk_dir, exist_ok=True)
        nltk.data.path.append(nltk_dir)
        nltk.download('vader_lexicon', download_dir=nltk_dir, quiet=True)  # download vader_lexicon for nltk lib


    def classified_tweets_emotions(self, data):
        actions = []
        for item in data:
            id = item["_id"]
            act = {
                "update":{
                    "_id": id,
                    "_index":"tweets"

                }
            }
            source = {
                "doc": {
                    "emotion": self._classified_emotion(item["_source"]["text"])
                }
            }
            actions.append(act)
            actions.append(source)

        return self.es.bulk(operations=actions)

    def _load_blacklist(self):
        black_list = []
        with open("data/weapon_list.txt", "r") as file:
            for line in file:
                black_list.append(line)
        return black_list

    def weapon(self, data):
        arr = []
        weapons = self._load_blacklist()
        for item in data:
            act = {
                "update": {
                    "_id": item["_id"],
                    "_index": "tweets"

                }
            }
            script = {
                "script": {
                    "lang": "painless",
                    "source": """
                                List weapons = params.weapons;
                                String text = ctx._source.text.toLowerCase();
                                ctx._source.weapon_detected = "no weapon here";
                                for (w in weapons) {
                                    if (text.contains(w.toLowerCase())) {
                                        ctx._source.weapon_detected = w;
                                        break
                                    }
                                }
                            """,
                    "params": {
                        "weapons": weapons
                    }
                }
            }
            arr.append(act)
            arr.append(script)
        return self.es.bulk(operations=arr)









