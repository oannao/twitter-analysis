# -*- coding: utf-8 -*-

from requests_oauthlib import OAuth1Session
from requests.exceptions import ConnectionError, ReadTimeout, SSLError
import json, datetime, time, pytz, re, sys, traceback, pymongo
from pymongo import MongoClient
from collections import defaultdict


KEYS = {'consumer_key': 'REPLACE_WITH_VALUE',
        'consumer_secret': 'REPLACE_WITH_VALUE',
        'access_token': 'REPLACE_WITH_VALUE',
        'access_secret': 'REPLACE_WITH_VALUE'}


def initialize():
    global twitter, connect, db, tweetdata, meta
    twitter = OAuth1Session(KEYS['consumer_key'], KEYS['consumer_secret'],
                            KEYS['access_token'], KEYS['access_secret'])
    connect = MongoClient('localhost', 27017)
    db = connect.sushi
    tweetdata = db.tweetdata
    meta = db.metadata


def get_tweet(search_word, max_id, since_id):
    global twitter
    url = 'https://api.twitter.com/1.1/search/tweets.json'
    params = {'q': search_word,
              'count': '100'}
    if max_id != -1:
        params['max_id'] = max_id
    if since_id != -1:
        params['since_id'] = since_id

    req = twitter.get(url, params=params)

    if req.status_code == 200:
        timeline = json.loads(req.text)
        metadata = timeline['search_metadata']
        statuses = timeline['statuses']
        limit = req.headers['x-rate-limit-remaining'] if 'x-rate-limit-remaining' in req.headers else 0
        reset = req.headers['x-rate-limit-reset'] if 'x-rate-limit-reset' in req.headers else 0
        return {"result": True, "metadata": metadata, "statuses": statuses, "limit": limit,
                "reset_time": datetime.datetime.fromtimestamp(float(reset)), "reset_time_unix": reset}
    else:
        print("Error: %d" % req.status_code)
        return{"result": False, "status_code": req.status_code}


def str_date_jp(str_date):
    dts = datetime.datetime.strptime(str_date, '%a %b %d %H:%M:%S +0000 %Y')
    return pytz.utc.localize(dts).astimezone(pytz.timezone('Asia/Tokyo'))


def get_unix_time():
    return time.mktime(datetime.datetime.now().timetuple())


if __name__ == "__main__":
    global tweetdata, meta
    sid, mid = -1, -1
    count = 0
    initialize()
    while(True):
        try:
            count = count + 1
            print("%d, " % count, end="")
            res = get_tweet(u'寿司', max_id=mid, since_id=sid)
            if res['result'] == False:
                print("status_code", res['status_code'])
                break

            if int(res['limit']) == 0:
                print("Adding created_at field.")
                for d in tweetdata.find({'created_datetime': {"$exists": False}}, {'_id': 1, 'created_at': 1}):
                    tweetdata.update({'_id': d['_id']},
                                     {'$set': {'created_datetime': str_date_jp(d['created_at'])}})

                diff_sec = int(res['reset_time_unix']) - get_unix_time()
                print("sleep %d sec." % (diff_sec+5))
                if diff_sec > 0:
                    time.sleep(diff_sec + 5)
            else:
                if len(res['statuses']) == 0:
                    print("statuses is none. ", end="")
                elif 'next_results' in res['metadata']:
                    meta.insert({"metadata": res['metadata'], "insert_date": get_unix_time()})
                    for s in res['statuses']:
                        tweetdata.insert(s)
                    next_url = res['metadata']['next_results']
                    pattern = r".*max_id=([0-9]*)\&.*"
                    ite = re.finditer(pattern, next_url)
                    for i in ite:
                        mid = i.group(1)
                        break
                else:
                    print("finished.", end="")
                    break
        except SSLError as errno:
            print("SSLError({0}): {1}".format(errno, strerror))
            print("waiting 5mins")
            time.sleep(5*60)
        except ConnectionError as errno:
            print("ConnectionError({0}): {1}".format(errno, strerror))
            print("waiting 5mins")
            time.sleep(5*60)
        except ReadTimeout as errno:
            print("ReadTimeout({0}): {1}".format(errno, strerror))
            print("waiting 5mins")
            time.sleep(5*60)
        except:
            print("Unexpected error:", sys.exc_info()[0])
            traceback.format_exc(sys.exc_info()[2])
            raise
        finally:
            info = sys.exc_info()
