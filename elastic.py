import requests
import json, math
from elasticsearch import Elasticsearch, TransportError

# res=requests.get('http://192.168.4.228:9200')
# print(res.content)
from elasticsearch.exceptions import HTTP_EXCEPTIONS

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
        if i % 1000 == 0:
            print(i)

        if len(scroll['hits']['hits']) > 0:
            for hit in scroll['hits']['hits']:
                items = items + 1
        else:
            break

    except TransportError:
        break

    response = {}

print(i)
print(items)
