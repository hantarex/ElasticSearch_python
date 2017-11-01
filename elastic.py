import requests
import json, math, numbers

import time

import sys
from elasticsearch import Elasticsearch, TransportError, helpers


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
    # body = {
    #     "doc": {
    #         "weight": weight,
    #     }
    # }
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
    # result = es.update(index='full_addr', doc_type='addr', id=id, body=body)
    # result = es.bulk(index='full_addr', doc_type='addr', body=body)
    result = helpers.bulk(es, body)
    print(id, ": ", result)


def update_doc_bulk(es: Elasticsearch, id: []):
    # body = {
    #     "doc": {
    #         "weight": weight,
    #     }
    # }
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
    # result = es.update(index='full_addr', doc_type='addr', id=id, body=body)
    # result = es.bulk(index='full_addr', doc_type='addr', body=body)
    result = helpers.bulk(es, body)
    # print(id, ": ", result)


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
    # break
    response = {}

print(i)
print(items)
