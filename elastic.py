import requests
import json, math, numbers
from elasticsearch import Elasticsearch, TransportError


def get_weight_from_id(es: Elasticsearch, id: int):
    # print("id: ", id)
    weight = 0
    body = {
        "fields": ["fulladdr_full"],
        "offsets": "true",
        "payloads": "true",
        "positions": "true",
        "term_statistics": "true",
        "field_statistics": "true"
    }
    item = es.termvectors(index='full_addr', doc_type='addr', id=id, body=body)
    for term, termValue in item['term_vectors']['fulladdr_full']['terms'].items():
        try:
            int(term)
        except ValueError:
            weight = weight + termValue['doc_freq']
    return weight


def update_doc(es: Elasticsearch, id: int, weight: int):
    body = {
        "doc": {
            "weight": weight,
        }
    }
    result = es.update(index='full_addr', doc_type='addr', id=id, body=body)
    # print(id, ": ", result['result'])


I = 1000

requestAll = {
    "size": I,
    "query": {
        "match_all": {}
    }
}

es = Elasticsearch([{'host': '192.168.4.228', 'port': 9200}])
response = es.search(index='full_addr', body=requestAll, params={"scroll": "10m"})

allDocs = response['hits']['total']

pages = math.ceil(allDocs / I)

scrollId = response['_scroll_id']

print("All pages: ", pages)

items = 0

i = 0
while 1 == 1:
    try:
        if len(response) > 0:
            scroll = response
        else:
            scroll = es.scroll(scrollId, scroll='1m')
        i = i + 1
        if i % 100 == 0:
            print(i)

        if len(scroll['hits']['hits']) > 0:
            for hit in scroll['hits']['hits']:
                weight = get_weight_from_id(es, hit['_id'])
                update_doc(es, hit['_id'], weight)
                items = items + 1
        else:
            break

    except TransportError:
        break
    # break
    response = {}

print(i)
print(items)
