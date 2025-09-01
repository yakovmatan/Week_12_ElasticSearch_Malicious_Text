import os

from src.configuration import Configuration
from src.dal import Dal
from src.identifier import Identifier
from src.read_files import ReadFile


class Manager:

    def __init__(self):
        self.index_name = os.getenv('INDEX_NAME', 'tweets')
        self.es = Configuration().get_es()
        self.read_files = ReadFile()
        self.tweets_injected = self.read_files.read_csv_file("../data/tweets_injected.csv")
        self.weapon_list = self.read_files.read_txt_file("../data/weapon_list.txt")
        self.dal = Dal(self.es)
        self.identifier = Identifier()

    def set_data(self):
        # Entering data into Elasticsearch
        self.dal.index_documents(self.index_name, self.tweets_injected)
        #creaet index
        mapping = {
            "TweetID": {"type": "keyword"},
            "CreateDate": {"type": "keyword", "format": "yyyy-MM-dd HH:mm:ssXXX"},
            "Antisemitic": {"type": "boolean"},
            "text": {
                "type": "text",
                "fields": {
                    "keyword": {"type": "keyword", "ignore_above": 256}
                }
            }
        }
        self.dal.create_index(index_name=self.index_name, mapping=mapping)
        # Add a sentiment field
        self.dal.add_field(self.index_name, self.identifier.sentiment_of_text, "sentiment")
        """
        Identifying keywords from a list of weapons and adding
        a field that includes the weapons found
        """
        self.dal.add_field(self.index_name, self.identifier.weapon_in_text, "weapon")
        """
        Deleting records from ElasticSearch that are not classified as anti-Semitic, 
        do not contain weapons, and have a neutral or positive sentiment.
        """
        self.dal.delete_by_conditions(index_name=self.index_name, conditions={
            "antisemitic": False,
            "weapons": False,
            "sentiment": ["neutral", "positive"]
        })
