import pdb
import json
from urllib import urlencode
from urllib2 import urlopen, URLError
from multiprocessing import Process, Array, JoinableQueue
from Queue import Queue
from threading import Thread
from Stemmer import Stemmer

def gen_stops():
    english_ignore = []
    with open('stoplist.txt',  'r') as stops:
        for word in stops:
            english_ignore.append(Array('u', word.strip(), lock=True))
    return frozenset(english_ignore)

def coroutine(func):
        def start(*args,**kwargs):
            g = func(*args,**kwargs)
            g.next()
            return g
        return start


class StringEater(object):
    
    def __init__(self):
        self.stoplist = gen_stops()
        self.stemmer = Stemmer('english')

    def eat_text(self, counter, ngram_size=2):
        return self.sent_filter(self.word_filter(
            (self.word_store(counter),self.ngram_count(counter,ngram_size)
                )))

    @coroutine
    def sent_filter(self,target):
        word = ''
        print "ready to eat lines"
        while True:
            sentence = (yield)
            target.send((sentence.lower()).split())

    @coroutine
    def word_filter(self, targets):
        print "ready to eat words"
        while True:
            raw = (yield)
            for t in targets:
                t.send([self.stemmer.stemWord(w) for w 
                    in raw if len(w)<=3 or w in self.stoplist])

    @coroutine
    def word_count(self, counter):
        while True:
            words = (yield)
            counter[word] += 1

    @coroutine
    def ngram_count(self,counter, n=2,):
        "Compute n-grams" 
        while True:
            grams= (yield)
            for i in range(0, len((grams)) - (n - 1)):
                counter[(tuple(grams[i:i+n]))] += 1
               
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


class RedisStringEater(StringEater):
    # TODO: Implement Redis hash and smembers or something

    def __init__(self):
        StringEater.__init__(self)

    def eat_text(self, counter, ngram_size=2):
        return self.sent_filter(self.word_filter(
            (self.word_store(),self.ngram_count(ngram_size))))

    @coroutine
    def word_count(self):
        while True:
            words = (yield)
            #counter[word] += 1

    @coroutine
    def ngram_count(self, n=2,):
        "Compute n-grams" 
        while True:
            grams= (yield)
            for i in range(0, len((grams)) - (n - 1)):
               # counter[(tuple(grams[i:i+n]))] += 1
               

class TwitterConsumer(Process, StringEater):
    """ Consumer process that will extract data given a Joinable queue """

    def __init__(self, q,stops=None, stemmer=None):
        Process.__init__(self)
        StringEater.__init__(self)
        self.input_q = q
        self.daemon = True
      
    @coroutine
    def tweet_filter(self, target):
        while True:
            sentences = (yield)
            for sent in sentences['results']:
                # Dont want retweets 
                if not sent['text'].startswith('RT'):
                    new_sent = sent['text'] 
                    # Test if their is urls in string, if so remove 
                    if sent['entities'].get('urls', False):
                        new_sent = sent['text']
                        for urls in sent['entities'].get('urls'):
                            i = urls['indices']
                            # Hackish with indices, but it works :)
                            new_sent = new_sent.replace(
                                    new_sent[i[0]:i[1]],'')
                    target.send(new_sent)



    def run(self):
        while True:
            data = self.input_q.get()
            print ('Exited run of data consumer')
            self.input_q.task_done()

    
class Gatherer(Thread):
    """Base threaded gatherer, Must overide run()"""

    def __init__(self):
        Thread.__init__(self)
        self.queue = JoinableQueue()

    def send(self, data):
        self.queue.put(data)

    def close(self):
        self.queue.put(None)
        self.queue.join()
    def run():
        pass


class TwitGather(Gatherer):
    """ Consumer of words that you want to extract from twitter. 
        Send it words in a tuple to query for and it will collect them 
        """

    def __init__(self,outq, pages=1):
        Gatherer.__init__(self)
        self.outq = outq
        self.pages = pages
        self.base = "http://search.twitter.com/search.json?"

    def run(self):
        while True:
            words = self.queue.get()
            if words is None:
                break

            options = {'lang':'en', 'result_type':'mixed',
                    'include_entities':1,'page':1, 'rpp':100,
                    'q': ' '.join(words)}
            try:
                for page in xrange(1, self.pages+1):
                    options['page'] = page
                    print('getting page %s' %page)
                    jfile = urlopen(self.base + urlencode(options),timeout=1) 
                    # TODO Need more specialed error checking
                    self.outq.put(json.load(jfile))
                print ('loaded data')
                self.queue.task_done()
            except (URLError, Exception) as e:
                print "failure in Twit gather url call "
                print e
                self.queue.task_done()
        self.queue.task_done()
        return

def twit_test():
    try:
        outqueue = JoinableQueue()
        cmonster = TwitterConsumer(outqueue)
        cmonster.start()
        twit = TwitGather(outqueue,1)
        twit.start() 
        twit.send(['words'])
        twit.send(['school'])
        twit.close()
        print "twit closed"
        outqueue.join()
        print ("cmonster closed")
    except Exception as e:
        print ("I failed")
        print e
   

if __name__ == '__main__':
    twit_test()
    
