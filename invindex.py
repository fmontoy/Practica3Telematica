from mrjob.job import MRJob
from pymongo import MongoClient
import unicodedata
import os

def mongo():
    client = MongoClient()
    db = client['hadoop']
    collection = db['words-books']

class MapReduce(MRJob):
    #Les quita las tildes a las letras de cada linea las pone en minuscula.
    h = {}

    def organizar(lista):
        return sorted(lista,key=getkey)

    def same(a):
        a = ''.join((c for c in unicodedata.normalize('NFD', a) if unicodedata.category(c) != 'Mn'))
        a = a.lower()
        return a

    def mapper(self,_,line):
        line = same(line)
        fileName = fileName = os.environ['map_input_file']

        for word in line:

            h[(word,fileName)] +=1

        for word in  h:
             yield

    def reducer():
        #Aqui hago el insert a mongo.
        yield


if __name__ == '__main__':
        mongo()
        MapReduce.run()
