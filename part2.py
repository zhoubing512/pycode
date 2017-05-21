# -*- coding:utf-8 -*-
import hashlib
import numpy as np
from sympy import *
from pymongo import MongoClient
from sympy.core.numbers import Float
from sklearn.pipeline import Pipeline
from elasticsearch import Elasticsearch
from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import PolynomialFeatures

def model_polyfit(Name,Source,Sepcy):
    inst = EsQuery()
    resultList = inst.querySingleTopic(Name, Source, Sepcy)
    yindex = resultList[0]
    tsstamp = resultList[1]
    y = []
    for i in range(len(yindex)):
        if i<len(yindex)-5 and yindex[i] <= yindex[i + 5]:
            y.append(yindex[i])
        else:
            x_endTime = i
            endTime = tsstamp[i]
            break
    if len(y)>0:
      y = np.array(y)
      x =[0]
      for ts in range(len(tsstamp)):
          if ts < len(tsstamp) - 1 and ts < len(y)-1:
              x_ts = round((tsstamp[ts + 1] - tsstamp[0]) / float(120),3)
              x.append(x_ts)
      x = np.array(x)

      degree = [4]
      for d in degree:
          clf = Pipeline([('poly', PolynomialFeatures(degree = d)),
                          ('linear', LinearRegression(fit_intercept = False))])
          clf.fit(x[:, np.newaxis], y)
          modelCoef = clf.named_steps['linear'].coef_
          modelCoef_str = ','.join(str(i) for i in clf.named_steps['linear'].coef_)

      x = symbols('x')
      y = modelCoef[0] + modelCoef[1] * x + modelCoef[2] * (x ** 2) + modelCoef[3] * (x ** 3) + modelCoef[4] * (x ** 4)
      startTime = solve(y, x, domain = S.Reals)
      xsTime = []
      for sTime in startTime:
          if isinstance(sTime, Float):
              xsTime.append(sTime)
      absstartTime = list(map(abs, xsTime))
      if len(absstartTime) >0 :
          for x_lab in xsTime:
              if abs(x_lab)==min(absstartTime):
                  xlab_start = x_lab
          duration = x_endTime - xlab_start
          result =  [modelCoef_str,Source,str(xlab_start),endTime,"polynomial",str(duration),Name,Sepcy]
          mongo_store(result)

class EsQuery:
    '''
    classdocs
    '''
    def __init__(self):
        '''
        Constructor
        '''
        self.es = Elasticsearch([{'host':'172.16.0.143','port':9200}])

    def querySingleTopic(self, name, source, specy):
        es_query = {
            "query":
            {
                "bool":
                {
                    "must":
                    [
                        {
                            "terms":{"name":[name]}
                        },
                         {
                            "terms":{"species":[specy]}
                        },
                        {
                            "terms":{"sources":[source]}
                        }
                    ]
                }
            },
            "size":10000,
            "sort":
            [
                {
                    "tsTime":{"order":"asc"}
                }
            ]
        }
        res = self.es.search(index="ranking-*", doc_type="ranking", body=es_query)
        indexList = []
        tsList = []
        for src in res['hits']['hits']:
            indexList.append(src['_source']['index'])
            tsList.append(src['_source']['ts'])
        return [indexList, tsList]

def mongo_store(parameters):
    client = MongoClient('csr.zintow.com:27017')
    db = client.the_database
    db.authenticate('admin','@xwmDP9115',source='csss')
    tdb = client.csss
    tdb_trend = tdb.model
    id = md5_encode(parameters[6]+parameters[1]+parameters[7])
    model = {"coefficient":parameters[0],"source":parameters[1],"intercept":parameters[2],"ts":parameters[3],"type":parameters[4],"interval":parameters[5],"name":parameters[6],"specy":parameters[7],"_id":id}
    tdb_trend.save(model)

def md5_encode(data):
    m5 = hashlib.md5()
    m5.update(data)
    data_md5 = m5.hexdigest()
    return data_md5
