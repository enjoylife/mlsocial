from multiprocessing import Process, Array, JoinableQueue
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
 
    def sent_filter(self):
        sentence = None
        while True:
            sentence = (yield word)
            word = [w for w in text if len(w) > 2 and w not in self.stoplist]
    
 
class TextEater(object):
    
    def __init__(self):
        self.stoplist = gen_stops()
        self.stemmer = Stemmer('english')
    
    @coroutine
    def sent_filter(self,target):
        print "ready to eat lines"
        while True:
            sentence = (yield)
            for word in sentence.split():
                target.send(word)

    @coroutine
    def word_filter(self):
        print "ready to eat words"
        while True:
            raw = (yield)
            if raw not in self.stoplist:
                print self.stemmer.stemWord(raw)
            
    @coroutine
    def typer(self):
        print "ready to check type"
        word = None
        while True:
            line = (yield word)
            word=  type(line)

          
if __name__ == '__main__':
    text_eater = TextEater()
    s = text_eater.sent_filter(text_eater.word_filter())
    t = text_eater.typer()



