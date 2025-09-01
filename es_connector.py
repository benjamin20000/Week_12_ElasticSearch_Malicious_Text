from elasticsearch import Elasticsearch, helpers


class EsConnect:
    def __init__(self):
        self.es = Elasticsearch("http://localhost:9200")

    def create_schema(self):
        mapping = {
            "properties": {
                "TweetID1": {"type": "float"},
                "CreateDate": {"type": "text"},
                "Antisemitic": {"type": "integer"},
                "text": {"type": "text"}
            }
        }
        index_name = "my_documents"
        self.es.indices.create(index=index_name, body={"mappings": mapping}, ignore=400)

    def insert_df(self,df):
        for index, row in df.iterrows():
            try:
                self.es.index(index='my_documents', body=row.to_dict())
            except Exception as e:
                print(e)



