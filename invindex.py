from mrjob.job import MRJob
from pymongo import MongoClient
import unicodedata
import os
import sys
import re
h = {}

def mongo():
    client = MongoClient()
    db = client['hadoop']
    collection = db['words-books']

def elimina_tildes(s):
   return ''.join((c for c in unicodedata.normalize('NFD', s) if unicodedata.category(c) != 'Mn'))

def same(a):
    a = elimina_tildes(a.decode('utf-8'))
    a = a.lower()
    a = a.split()
    return a

def getkey(item):
    return item[1]

def organizar(lista):
    return sorted(lista, key=getkey)

class MapReduce(MRJob):

    def mapper(self,_,line):
        line = same(line)
        fileName = os.environ['map_input_file']
        item = ""
        for item in line:
            if item in h:
                h[(item,fileName)] +=1
            else:
                h[(item,fileName)] =1
        yield (item,fileName)

    def reducer(self,item,fileName):
        p ={}
        item = ""

        for item in h:
            p[item] = (item,fileName)

        p = organizar(p)
        #Aqui hago el insert a mongo.

        yield (item,p)


if __name__ == '__main__':
        mongo()
        MapReduce.run()
