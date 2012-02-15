from urllib import urlencode
import json
from urllib2 import urlopen, URLError
from Queue import Queue
from threading import Thread
class TwitGather(Thread):
    """ Consumer of words that you want to extract from twitter. 
        Send it words to query for and it will collect them 
        """

    def __init__(self, page=1):
        Thread.__init__(self)
        self.page = page
        self.base = "http://search.twitter.com/search.json?"
        self.queue = Queue()


    def send(self, data):
        self.queue.put(data)

    def close(self):
        self.queue.put(None)
        self.queue.join()

    def run(self):
        while True:
            words = self.queue.get()
            if words is None:
                break
            options = {'lang':'en', 'result_type':'mixed',
                    'include_entities':1,'page':self.page, 'rpp':100,
                    'q': ' '.join(words)}
            try:
                data= urlopen(self.base + urlencode(options),timeout=.5) 
                json.load(data)
                print ('loaded data')
                # TODO Do something with the received json data
                self.queue.task_done()
            except URLError:
                self.queue.task_done()
        self.queue.task_done()
        return


if __name__ == '__main__':
    try:
        twit = TwitGather()
        twit.start() 
        twit.send(['words'])
        twit.send(['test'])
        twit.close()
        print ("I closed")
    except Exception:
        print ("I failed")
   
