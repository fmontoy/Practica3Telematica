
from mrjob.job import MRJob
from pymongo import MongoClient
import unicodedata
import os
import sys
import re

client = MongoClient('localhost',27017)
db = client['hadoop']
collection = db['coll']

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
        fileName = str(os.environ['map_input_file'])

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
        post = { "word" : item, "files" : str(l)}
        collection.insert_one(post)

        yield (item,l)

if __name__ == '__main__':

        MapReduce.run()
