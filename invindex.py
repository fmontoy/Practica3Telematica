
from mrjob.job import MRJob
from pymongo import MongoClient
import unicodedata
import os
import sys
import re

def mongo():
    client = MongoClient()
    db = client['hadoop']
    collection = db['words-books']

def elimina_tildes(s):
   return ''.join((c for c in unicodedata.normalize('NFD', s) if unicodedata.category(c) != 'Mn'))

def same(a):
    a = elimina_tildes(a.decode('utf-8'))
    a = a.replace(',','').replace('.','').replace(';','')
    a = a.lower()
    a = a.split()
    return a

class MapReduce(MRJob):

    def mapper(self,_,line):

        item = ""
        line = same(line)
        fileName = os.environ['map_input_file']
        
        for item in line:
            yield (item,fileName)

    def reducer(self,item,fileNames):

        p = {}
        count = 0
        i = ""
        for i in fileNames:
            if i not in p:
               p[i] = 1
            else:
               p[i] +=1

        l = p.items()
        l.sort(key=lambda x: x[1],reverse=True)

        yield (item,l)

if __name__ == '__main__':

        mongo()
        MapReduce.run()
