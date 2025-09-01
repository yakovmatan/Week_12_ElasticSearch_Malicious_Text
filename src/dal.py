from elasticsearch import Elasticsearch, helpers

class Dal:

    def __init__(self, connection: Elasticsearch):
        self.es = connection

    def create_index(self, index_name: str, mapping: dict):
        if not self.es.indices.exists(index=index_name):
            self.es.indices.create(index=index_name, mappings={
                "properties": mapping
            })
            print(f"Index {index_name} created successfully")
        else:
            print(f"Index {index_name} already exists")

    def index_documents(self, index_name: str, data: list):
        actions = []
        for doc in data:
            action = {
                "_index": index_name,
                "_source": doc
            }
            actions.append(action)

        try:
            success, failed = helpers.bulk(self.es, actions)
            print(f"Successfully indexed {success} documents, failed: {len(failed)}")
        except Exception as e:
            print(f"Failed to bulk index documents: {e}")

    def add_field(self, index_name: str, value_extractor, field_name, *args):
        print("start add field")
        try:
            res = helpers.scan(self.es, index=index_name, query={"query": {"match_all": {}}})
        except Exception as e:
            print(f"Failed to read from index {index_name}: {e}")
            return
        try:
            bulk_actions = []
            for hit in res:
                doc_id = hit["_id"]
                body_text = hit["_source"]["text"]
                new_field = value_extractor(body_text, *args)

                action = {
                    "_op_type": "update",
                    "_index": index_name,
                    "_id": doc_id,
                    "doc": {field_name: new_field}
                }
                bulk_actions.append(action)
            helpers.bulk(self.es, bulk_actions)
            print(f"Documents update")
        except Exception as e:
            print(f"Failed to update doc: {e}")

    def delete_by_conditions(self, index_name: str, conditions: dict):
        try:
            print("start to delete")
            conditions_list = []
            for field, value in conditions.items():
                if isinstance(value, list):
                    conditions_list.append({"terms": {field: value}})
                else:
                    conditions_list.append({"term": {field: value}})

            query = {"bool": {"must": conditions_list}}

            res = self.es.delete_by_query(index=index_name, query=query)
            return res["deleted"]
        except Exception as e:
            print(f"Failed to delete documents from index {index_name}: {e}")
            return 0