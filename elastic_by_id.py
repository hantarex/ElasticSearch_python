import requests
import json, math, numbers

import time

import sys
from elasticsearch import Elasticsearch, TransportError, helpers


def get_weight_from_id(es: Elasticsearch, id: int):
    # print("id: ", id)
    weight_doc = 0
    body = {
        "fields": ["fulladdr_full", "fulladdr_full_nw"],
        "offsets": "true",
        "payloads": "true",
        "positions": "true",
        "term_statistics": "true",
        "field_statistics": "true"
    }
    item = es.termvectors(index='full_addr', doc_type='addr', id=id, body=body)

    try:
        for term, termValue in item['term_vectors']['fulladdr_full_nw']['terms'].items():
            try:
                int(term)
            except ValueError:
                weight_doc = weight_doc + termValue['doc_freq']
        if len(item['term_vectors']['fulladdr_full_nw']['terms'])>1:
            delimiter = len(item['term_vectors']['fulladdr_full_nw']['terms']) * 5
        else:
            delimiter = len(item['term_vectors']['fulladdr_full_nw']['terms'])

        weight_doc = weight_doc / delimiter
        return weight_doc
    except KeyError:
        print("Error: id (", id, ") dont't have key")
        return 0


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

id = 553513

es = Elasticsearch([{'host': '192.168.4.228', 'port': 9200}])
weight = get_weight_from_id(es, id)

update_doc_bulk(es, [{'id': id, 'weight': weight}])

print(weight)
