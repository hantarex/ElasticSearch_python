import requests
import json, math, numbers

import time

import sys
from elasticsearch import Elasticsearch, TransportError, helpers

ANALYZE_FIELD = "fulladdr_implode"

I1 = 10
I = 1000


def get_weight_from_id(es: Elasticsearch, id: int):
    weight_doc = 0
    body = {
        "fields": [ANALYZE_FIELD],
        "offsets": "true",
        "payloads": "true",
        "positions": "true",
        "term_statistics": "true",
        "field_statistics": "true"
    }
    item = es.termvectors(index='full_addr', doc_type='addr', id=id, body=body)

    try:
        for term, termValue in item['term_vectors'][ANALYZE_FIELD]['terms'].items():
            try:
                int(term)
            except ValueError:
                weight_doc = weight_doc + termValue['doc_freq']
        if len(item['term_vectors'][ANALYZE_FIELD]['terms']) > 1:
            delimiter = len(item['term_vectors'][ANALYZE_FIELD]['terms']) * I1
        else:
            delimiter = len(item['term_vectors'][ANALYZE_FIELD]['terms'])

        weight_doc = weight_doc / delimiter
        return weight_doc
    except KeyError:
        print("Error: id (", id, ") dont't have key")
        return 0


def update_doc(es: Elasticsearch, id: int, weight: int):
    body = [
        {
            '_op_type': 'update',
            '_index': 'full_addr',
            '_type': 'addr',
            '_id': id,
            'doc': {
                "weight": 123
            }
        }
    ]
    result = helpers.bulk(es, body)
    print(id, ": ", result)


def update_doc_bulk(es: Elasticsearch, id: []):
    body = [
        {
            '_op_type': 'update',
            '_index': 'full_addr',
            '_type': 'addr',
            '_id': val['id'],
            'doc': {
                "weight": val['weight']
            }
        }
        for val in id
    ]
    result = helpers.bulk(es, body)


requestAll = {
    "size": I,
    "query": {
        "match_all": {}
    }
}

es = Elasticsearch([{'host': '192.168.4.228', 'port': 9200}])
response = es.search(index='full_addr', body=requestAll, params={"scroll": "30m"})
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

        sys.stdout.write('\r' + str(i))
        sys.stdout.flush()
        if len(scroll['hits']['hits']) > 0:
            id_mass = []
            for hit in scroll['hits']['hits']:
                weight = get_weight_from_id(es, hit['_id'])
                # update_doc(es, hit['_id'], weight)
                items = items + 1
                # print("Items updated: ", items)

                # print(items, end='\r')
                # time.sleep(1)
                id_mass.append({'id': hit['_id'], 'weight': weight})
            update_doc_bulk(es, id_mass)
        else:
            break

    except TransportError:
        break
    response = {}

print(i)
print(items)
