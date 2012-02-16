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
            english_ignore.append(Array('u', word.strip(), lock=False))

def coroutine(func):
        def start(*args,**kwargs):
            g = func(*args,**kwargs)
            g.next()
            return g
        return start


class DataConsumer(Process):
    """ Consumer process that will extract data given a Joinable queue """

    def __init__(self, q, stemmer=None):
        Process.__init__(self)
        self.input_q = q
        self.daemon = True

        if not stemmer: 
            from Stemmer import Stemmer 
            self.stemmer = Stemmer('english')

        self.stoplist = gen_stops()


    def text_filter():
        pass

    def run(self):
        while True:
            data = self.input_q.get()
            # Replace with real extraction work later
            print ('entered run of data consumer')
            self.input_q.task_done()

    
class Gatherer(Thread):
    """Base threaded gatherer"""

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

    def __init__(self,outq, page=1):
        Gatherer.__init__(self)
        self.outq = outq
        self.page = page
        self.base = "http://search.twitter.com/search.json?"

    def run(self):
        while True:
            words = self.queue.get()
            if words is None:
                break
            options = {'lang':'en', 'result_type':'mixed',
                    'include_entities':1,'page':self.page, 'rpp':100,
                    'q': ' '.join(words)}
            try:
                jfile = urlopen(self.base + urlencode(options),timeout=.5) 
                fp =jfile.read()
                self.outq.put(fp)
                print fp[0:22]
                print ('loaded data')
                self.queue.task_done()
            except Exception as e:
                print "failure in Twit gather url call "
                print e
                self.queue.task_done()
        self.queue.task_done()
        return

def twit_test():
    try:
        outqueue = JoinableQueue()
        twit = TwitGather(outqueue)
        twit.start() 
        twit.send(['words'])
        twit.close()
        print "twit closed"
        cmonster = DataConsumer(outqueue)
        cmonster.start()
        outqueue.join()
        print ("cmonsterclosed closed")
    except Exception as e:
        print ("I failed")
        print e
   

if __name__ == '__main__':
    twit_test()
    
