# -*- coding:utf-8 -*-
from elasticsearch import Elasticsearch
from part2 import *

def ranking_query():
    sources = ['weibo']
    species = ['实时','热点','人物','潮流']
    es = Elasticsearch([{'host':'172.16.0.143','port':9200}])
    for source in sources:
        for specy in species:
            print source,specy
            es_query = {"query":{"bool":{"must":[{"terms":{"sources":[source]}},{"terms":{"species":[specy]}}]}},"aggs":{"NAME":{"max":{"field":"tsTime"}}},"size":0}
            res = es.search(index="ranking-*", doc_type="ranking", body=es_query)
            currentTs = res['aggregations']['NAME']['value_as_string']
            es_query = {"query":{"bool":{"must":[{"terms":{"sources":[source]}},{"terms":{"species":[specy]}},{"term":{"tsTime":{"value":currentTs.encode('utf-8')}}}]}},"size":1000}
            res = es.search(index="ranking-*", doc_type="ranking", body=es_query)
            for hit in res['hits']['hits']:
                name = hit['_source']['name'].encode('utf-8')
                #print name
                model_polyfit(name, source, specy)

ranking_query()
