import pdb
import json
from urllib import urlencode
from urllib2 import urlopen, URLError
from multiprocessing import Process, Array, JoinableQueue
from Queue import Queue
from threading import Thread

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


class TwitterConsumer(Process):
    """ Consumer process that will extract data given a Joinable queue """

    def __init__(self, q,stops=None, stemmer=None):
        Process.__init__(self)
        self.input_q = q
        self.daemon = True

        if not stemmer: 
            from Stemmer import Stemmer 
            self.stemmer = Stemmer('english')

        if not stops: self.stoplist = gen_stops()
 
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
        word = None
        while True:
            raw = (yield)
            if raw not in self.stoplist:
                print self.stemmer.stemWord(raw)
       
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
            flow = self.tweet_filter(self.sent_filter(self.word_filter()))
            data = self.input_q.get()
            flow.send(data)
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
        Send it words to query for and it will collect them 
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
                    jfile = urlopen(self.base + urlencode(options),timeout=.5) 
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
    
