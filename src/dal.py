from elasticsearch import Elasticsearch


class Dal:

    def __init__(self, connection: Elasticsearch):
        self.es = connection

    def create_index(self, index_name: str, mapping: dict):
        if not self.es.indices.exists(index=index_name):
            self.es.indices.create(index=index_name, mappings={
                "properties": mapping
            })

    def index_documents(self, index_name: str, data: list):
        for doc in data:
            res = self.es.index(index=index_name, document=doc)
            print(f"result={res['result']}")

    def add_field(self, index_name: str, value_extractor, field_name):
        res = self.es.search(index=index_name, query={"match_all": {}}, size=1000)

        for hit in res["hits"]["hits"]:
            doc_id = hit["_id"]
            body_text = hit["_source"]["body"]
            new_field = value_extractor(body_text)

            self.es.update(
                index=index_name,
                id=doc_id,
                doc={field_name: new_field}
            )

    def delete_by_conditions(self, index_name: str, conditions: dict):
        conditions_list = []

        for field, value in conditions.items():
            if isinstance(value, list):
                conditions_list.append({"terms": {field: value}})
            else:
                conditions_list.append({"term": {field: value}})

        query = {"bool": {"must": conditions_list}}

        res = self.es.delete_by_query(index=index_name, query=query)
        return res["deleted"]
