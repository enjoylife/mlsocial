# -*- coding: utf-8 -*-
from multiprocessing import  Array 
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
            if raw not in self.stoplist and len(raw) > 2:
                target.send(self.stemmer.stemWord(raw))

    @coroutine
    def ngrams(self,target, n=2, padding=False):
        "Compute n-grams with optional padding"
        while True:
            words= (yield)
            pad = [] if not padding else [None]*(n-1)
            grams = pad + words + pad
            for i in range(0, len(grams) - (n - 1)):
                target.send(tuple(grams[i:i+n]))
               
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
    t = TextEater()
    s = t.sent_filter(t.ngrams(t.printer()))
    s.send('This is a test sentence and that has lots of words')



