# -*- coding: UTF-8 -*-
import Stemmer
#twitter_search
import requests
from requests import RequestException
from requests import async
from urllib import urlencode
from urllib2 import urlopen
import json
from multiprocessing import Process
from thread import Thread

#redisbayes
import re
import math
import redis
import random
from redis import WatchError

stemmer = Stemmer.Stemmer('english')
english_ignore = set()
with open('stoplist.txt',  'r') as stops:
    for word in stops:
        english_ignore.add(stemmer.stemWord(word.strip()))

class TwitGather(Thread):
    """ Consumer of words that you want to extract from twitter. 
        Send it words to query for and it will collect them 
        """

    def __init__(self, q,  page=1):
        Thread.__init__(self)
        self.page = page
        self.base = "http://search.twitter.com/search.json?"
        self.queue = q


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
                    'include_entities':1,'page':page, 'rpp':100,
                    'q': ' '.join(words)}
            try:
                with urlopen(self.base + urlencode(options),timeout=.5) as data:
                    json.load(data)
                    # TODO Do something with the received json data
            except URLError:

                pass
            self.queue.task_done()
            return


class DataConsumer(Process):
    """ Consumer process that will extract data given a Joinable queue """

    def __init__(self, q, stoplist, stemmer, data):
        Process.__init__(self)
        self.input_q = q
        self.stemmer = stemmer 
        self.stoplist = stoplist
        self.data = data

    def run(self):
        while True:
            data = self.input_q.get()
            # Replace with real extraction work later
            print data 
            self.input_q.task_done()


class RedisExtract(object):

    def __init__(self, redis=None, prefix='data:'):
        if not redis: import redis; self.redis = redis.Redis()
        else: self.redis = redis
        self.prefix = prefix
        
    
    def flush(self):
        for cat in self.redis.smembers(self.prefix + 'categories'):
            self.rpipe.delete(self.prefix + cat)
        self.rpipe.delete(self.prefix + 'categories')
        self.rpipe.execute()

    def train(self, category, text):
        self.redis.sadd(self.prefix + 'categories', category)
        ## TODO create a super class that has a whole set of text extraction
        #tools
    
def tidy(text):
    if not isinstance(text, basestring):
        text = str(text)
    if not isinstance(text, unicode):
        text = text.decode('utf8')
    text = text.lower()
    return re.sub(r'[\_.,<>:;~+|\[\]?`"!@#$%^&*()\s]', ' ', text, re.UNICODE)



def english_tokenizer(text, stem=True, stops=english_ignore):

    if stem:
        words = stemmer.stemWords(tidy(text).split())
    else:
        words = text.split()
    return [w for w in words if len(w) > 2 and w not in stops ]


def occurances(words):
    counts = {}
    for word in words:
        if word in counts:
            counts[word] += 1
        else:
            counts[word] = 1
    return counts


class RedisBayes(object):
    u""" Naïve Bayesian Text Classifier on Redis.

    redisbayes
    ~~~~~~~~~~


    I wrote this to filter spammy comments from a high traffic forum website
    and it worked pretty well.  It can work for you too :)

    For example::

        >>> rb = RedisBayes(redis.Redis(), prefix='bayes:test:')
        >>> rb.flush()
        >>> rb.classify('nothing trained yet') is None
        True
        >>> rb.train('good', 'sunshine drugs love sex lobster sloth')
        >>> rb.train('bad', 'fear death horror government zombie god')
        >>> rb.classify('sloths are so cute i love them')
        'good'
        >>> rb.classify('i fear god and love the government')
        'bad'
        >>> int(rb.score('i fear god and love the government')['bad'])
        -9
        >>> int(rb.score('i fear god and love the government')['good'])
        -14
        >>> rb.untrain('good', 'sunshine drugs love sex lobster sloth')
        >>> rb.untrain('bad', 'fear death horror government zombie god')
        >>> rb.score('lolcat')
        {}

    Words are lowercased and unicode is supported::

        >>> print tidy(english_tokenizer("Æther")[0])
        æther

    Common english words and 1-2 character words are ignored::

        >>> english_tokenizer("greetings mary a b aa bb")
        [u'mari']

    Some characters are removed::

        >>> print english_tokenizer("contraction's")[0]
        contract
        >>> print english_tokenizer("what|is|goth")[0]
        goth

    """


    def __init__(self, redis=None, prefix='bayes:', correction=0.1,
                 tokenizer=None):
        self.redis = redis
        self.prefix = prefix
        self.correction = correction
        self.tokenizer = tokenizer or english_tokenizer
        if not self.redis:
            import redis
            redis = redis.Redis()
        self.rpipe = redis.pipeline()


    def flush(self):
        for cat in self.redis.smembers(self.prefix + 'categories'):
            self.rpipe.delete(self.prefix + cat)
        self.rpipe.delete(self.prefix + 'categories')
        self.rpipe.execute()

    def train(self, category, text, twitdata=False, multi=False):

        self.redis.sadd(self.prefix + 'categories', category)
        words = self.tokenizer(text)

        for word, count in occurances(words).iteritems():
            self.rpipe.hincrby(self.prefix + category, word, count)
        self.rpipe.execute()

        if twitdata and not multi:

            words = set(self.tokenizer(text, stem=False))
            sample = random.sample(words, 2) 

            tweets = twitter_search(sample, twit_pages=5)
            data = self.tokenizer(tweets,stops=(words|english_ignore))

            for word, count in occurances(data).iteritems():
                self.rpipe.hincrby(self.prefix + category, word, count)
            self.rpipe.execute()
        if multi:
            words = set(self.tokenizer(text, stem=False))
            sample = random.sample(words, 2) 

            twitter_search_async(sample, twit_pages=5)


    def untrain(self, category, text):
        for word, count in occurances(self.tokenizer(text)).iteritems():
            cur = self.redis.hget(self.prefix + category, word)
            if cur:
                new = int(cur) - count
                if new > 0:
                    self.redis.hset(self.prefix + category, word, new)
                else:
                    self.redis.hdel(self.prefix + category, word)
        with self.rpipe:
            while 1:
                try:
                    self.rpipe.watch(self.prefix + category)
                    if self.rpipe.hlen(self.prefix + category) == 0:
                        self.rpipe.multi()
                        self.rpipe.delete(self.prefix + category) 
                        self.rpipe.srem(self.prefix + 'categories', category)
                        self.rpipe.execute()
                        break
                except WatchError:
                    continue

    def classify(self, text):
        score = self.score(text)
        if not score:
            return None
        return sorted(score.iteritems(), key=lambda v: v[1])[-1][0]#hackish?

    def guess(self, text):
        score = self.score(text)
        if not score:
            return None
        values = sorted(score.iteritems(), key=lambda v: v[1], reverse=True)#hackish?
        for key, value in values:
            print key


    def score(self, text):
        occurs = occurances(self.tokenizer(text))
        scores = {}
        for category in self.redis.smembers(self.prefix + 'categories'):
            tally = self.tally(category)
            scores[category] = 0.0
            for word, count in occurs.iteritems():
                score = self.redis.hget(self.prefix + category, word)
                assert not score or score > 0, "corrupt bayesian database"
                score = score or self.correction
                scores[category] += math.log(float(score) / tally)
        return scores

    def tally(self, category):
        tally = sum(int(x) for x in self.redis.hvals(self.prefix + category))
        assert tally >= 0, "corrupt bayesian database" #TODO better error check
        return tally


def twitter_search(words,  twit_pages):

    """input: list of words, returns a list of results 
    Filtering out the multiple retweets and named entities
    
    TODO: Error checking and better func params"""

    sentences=[]
    pagenum = twit_pages
    twit_base = "http://search.twitter.com/search.json?"
    for page in range(1,pagenum+1):
        twit_ops = {'lang':'en', 'result_type':'mixed','include_entities':1,'page':page, 'rpp':100,
                'q': ' '.join(words)}
        try:
            r =requests.get(twit_base+urllib.urlencode(twit_ops), prefetch=True,
                timeout=.4)
            content = json.loads(r.text)
        except RequestException:
            continue
        print "adding new sentences for page %s" % page
        for sents in content['results']:
            # Dont want retweets 
            if not sents['text'].startswith('RT'):
                new_sent = sents['text']
                # Test if their is urls in string, if so remove them before appending
                if sents['entities'].get('urls', False):
                    for urls in sents['entities'].get('urls'):
                        i = urls['indices']
                        # Hackish with indices, but it works :)
                        new_sent=sents['text'].replace(sents['text'][i[0]:i[1]],'')
                sentences.append(new_sent.encode('utf-8'))
    return sentences
red = redis.Redis()
rp = red.pipeline()
def twitter_search_async(words,  twit_pages):

    """input: list of words, returns a list of results 
    Filtering out the multiple retweets and named entities
    
    TODO: Error checking and better func params"""

    urls=[]
    pagenum = twit_pages
    twit_base = "http://search.twitter.com/search.json?"

    for page in range(1,pagenum+1):

        twit_ops = {'lang':'en', 'result_type':'mixed','include_entities':1,'page':page, 'rpp':100,
                'q': ' '.join(words)}
        urls.append(async.get(twit_base+urllib.urlencode(twit_ops), 
            timeout=.4, hooks={'response': parse_tweets}))
    async.map(urls) 

def parse_tweets(data):
    sentences=[]
    tweets = json.load(data.text)
    for sents in tweets['results']:
        print decoding

        # Dont want retweets 
        if not sents['text'].startswith('RT'):
            new_sent = sents['text']

            # Test if their is urls in string, if so remove them before appending
            if sents['entities'].get('urls', False):
                for urls in sents['entities'].get('urls'):
                    i = urls['indices']

                    # Hackish with indices, but it works :)
                    new_sent=sents['text'].replace(sents['text'][i[0]:i[1]],'')
            sentences.append(new_sent.encode('utf-8'))
    for word, count in occurances(sentences).iteritems():
        rp.hincrby(self.prefix + category, word, count)
    rp.execute()



if __name__ =='__main__':
    #import doctest
    #doctest.testmod()
    #rb = RedisBayes(redis.Redis(), prefix="bayes:test:")
    #rb.train('food', ' pizza apples yogurt water' , True,)
    #rb.train('sports', ' baseball hockey football soccer' , True)
    #rb.train('sports', ' bat glove goal field rink hoop helmet ' , True)
    #rb.train('school', ' paper pencil laptop hw' , True)
    #rb.train('book', 'read table of contents paper cover', True )
    #rb.train('car', 'ford nissan gmc wheel headlight ', True)
#    rb.train('math', ' subtract times multiple divide log ', True , True)
#    print rb.classify('sit down at the table')
#    print rb.classify(' i read')
#    print rb.classify('puck')
#    print rb.classify('honda')
#    print rb.classify('division')


