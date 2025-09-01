import os
import time
from elasticsearch import Elasticsearch


class Configuration:

    def __init__(self):
        conn = os.getenv('ES_HOST','localhost')
        self.es:Elasticsearch = Elasticsearch(f'http://{conn}:9200')
        self.__availability_check()

    def get_es(self) -> Elasticsearch :
        return self.es

    def __availability_check(self):
        while True:
            try:
                if self.es.ping():
                    return "Elasticsearch is ready"

            except ConnectionError:
                pass
            time.sleep(2)