# -*- coding: utf-8 -*-
from collections import Counter
from multiprocessing import  Array ,Queue
from Stemmer import Stemmer

def coroutine(func):
        def start(*args,**kwargs):
            g = func(*args,**kwargs)
            g.next()
            return g
        return start


def gen_stops():
    english_ignore = []
    with open('stoplist.txt',  'r') as stops:
        for word in stops:
            english_ignore.append(Array('u', word.strip(), lock=True))
    return frozenset(english_ignore)

class TextEater(object):
    
    def __init__(self):
        self.stoplist = gen_stops()
        self.stemmer = Stemmer('english')
    
    @coroutine
    def sent_filter(self,target):
        word = ''
        print "ready to eat lines"
        while True:
            sentence = (yield)
            target.send((sentence.lower()).split())

    @coroutine
    def word_filter(self, target):
        print "ready to eat words"
        while True:
            raw = (yield)
            target.send([self.stemmer.stemWord(w) for w in raw if len(w)<=3 or 
                    w in self.stoplist])


    @coroutine
    def ngrams(self,container, n=2,):
        "Compute n-grams" 
        while True:
            grams= (yield)
            for i in range(0, len((grams)) - (n - 1)):
                container[(tuple(grams[i:i+n]))]+=1
               
    @coroutine
    def printer(self):
        while True:
            line = (yield)
            print (line)

    @coroutine
    def typer(self,target):
        print "ready to check type"
        word = None
        while True:
            line = (yield word)
            word=  type(line)


if __name__ == '__main__':
    cnt = Counter()
    t = TextEater()
    s = t.sent_filter(t.word_filter(t.ngrams(cnt)))
    #s = t.sent_filter(t.printer())
    s.send("this is the best senence ever for testing")
    print cnt



