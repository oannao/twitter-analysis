# -*- coding: utf-8 -*-

import json, datetime, time, pytz, re
from pymongo import MongoClient
from collections import defaultdict
from collections import OrderedDict


date_dict = defaultdict(int)
ret_date_dict = defaultdict(int)
norm_date_dict = defaultdict(int)


def initialize():
    global connect, db, tweetdata, meta
    connect = MongoClient('localhost', 27017)
    db = connect.sushi
    tweetdata = db.tweetdata
    meta = db.metadata


def date_to_Japan_time(dts):
    return datetime.datetime.strptime(dts, "%a %b %d %H:%M:%S %z %Y").astimezone(pytz.timezone('Asia/Tokyo'))


if __name__ == "__main__":
    global db, tweetdata, meta
    initialize()
    for d in tweetdata.find({}, {'_id': 1, 'created_at': 1}):
        str_date = date_to_Japan_time(d['created_at']).strftime('%Y\t%m/%d %H %a')
        date_dict[str_date] += 1
        if 'retweeted_status' not in d:
            norm_date_dict[str_date] += 1
        elif obj_nullcheck(d['retweeted_status']):
            ret_date_dict[str_date] += 1
        else:
            norm_date_dict[str_date] += 1
    print("Date" + "\t\t\t" + "#ALL" + "\t" + "#NotRT" + "\t" + "#RT")
    ordered_date_dict = OrderedDict(sorted(date_dict.items(), key=lambda t: t[0]))
    for k in ordered_date_dict:
        print(k + "\t" + str(date_dict[k]) + "\t" + str(norm_date_dict[k])
              + "\t" + str(ret_date_dict[k]))
