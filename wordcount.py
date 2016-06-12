# -*- coding: utf-8 -*-

import json, datetime, time, pytz, re, sys
import unicodedata
import MeCab
from pymongo import MongoClient
from collections import defaultdict
from collections import OrderedDict
from sklearn.feature_extraction.text import CountVectorizer
import numpy as np


date_dict = defaultdict(int)
ret_date_dict = defaultdict(int)
norm_date_dict = defaultdict(int)
freq = defaultdict(int)

MECAB_MODE = '-Ochasen'
PARSE_TEXT_ENCODING = 'utf-8'


def initialize():
    global connect, db, tweetdata, meta
    connect = MongoClient('localhost', 27017)
    db = connect.sushi
    tweetdata = db.tweetdata
    meta = db.metadata


def mecab_analysis(sentence):
    tagger = MeCab.Tagger(MECAB_MODE)
    sentence = sentence.replace('\n', ' ')
    node = tagger.parseToNode(sentence)
    parsed_words_dict = defaultdict(list)
    while node:
        word_type = node.feature.split(",")[0]
        if word_type in ["名詞", "形容詞", "動詞"]:
            plain_word = node.feature.split(",")[6]
            if plain_word != "*":
                parsed_words_dict[word_type].append(plain_word)
        node = node.next
        if node is None:
            break
    return parsed_words_dict


def get_mecabed_strings():
    global tweetdata
    tweet_list = []
    tweet_texts = []
    for d in tweetdata.find({}, {'noun': 1, 'verb': 1, 'adjective': 1, 'adverb': 1, 'text': 1}):
        tweet = ""
        if 'noun' in d:
            for word in d['noun']:
                tweet += word + " "
        if 'verb' in d:
            for word in d['verb']:
                tweet += word + " "
        if 'adjective' in d:
            for word in d['adjective']:
                tweet += word + " "
        if 'adverb' in d:
            for word in d['adverb']:
                tweet += word + " "
        tweet_list.append(tweet)
        tweet_texts.append(d['text'])
    return {"tweet_list": tweet_list, "tweet_texts": tweet_texts}


def write_to_csv(data):
    with open('result/wordcount.csv', 'w') as f:
        writer = csv.writer(f, lineterminator='\n')
        writer.writerow(['Word', '#Word'])
        for word, count in data:
            writer.writerow([word, count])


def main():
    global tweetdata
    for d in tweetdata.find({}, {'_id': 1, 'id': 1, 'text': 1}):
        res = mecab_analysis(unicodedata.normalize('NFKC', d['text']))
        for k in res.keys():
            if k == '形容詞':
                adjective_list = []
                for w in res[k]:
                    adjective_list.append(w)
                    freq[w] += 1
                tweetdata.update({'_id': d['_id']}, {'$push': {'adjective': {'$each': adjective_list}}})
            elif k == '動詞':
                verb_list = []
                for w in res[k]:
                    verb_list.append(w)
                    freq[w] += 1
                tweetdata.update({'_id': d['_id']}, {'$push': {'verb': {'$each': verb_list}}})
            elif k == '名詞':
                noun_list = []
                for w in res[k]:
                    noun_list.append(w)
                    freq[w] += 1
                tweetdata.update({'_id': d['_id']}, {'$push': {'noun': {'$each': noun_list}}})
        tweetdata.update({'_id': d['_id']}, {'$set': {'mecabed': True}})
    ret_all = get_mecabed_strings()
    tw_list_all = ret_all['tweet_list']
    c_vec = CountVectorizer(stop_words=[u"寿司"])
    c_vec.fit(tw_list_all)
    c_terms = c_vec.get_feature_names()
    transformed = c_vec.transform(tw_list_all)
    arg_ind = np.argsort(transformed.toarray())[0][:-50:-1]
    genexp = ((k, freq[k]) for k in sorted(freq, key=freq.get, reverse=True)[0:100])
    write_to_csv(genexp)
    for k, v in genexp:
        print(k + '\t\t\t' + str(v))


if __name__ == "__main__":
    global db, tweetdata, meta
    initialize()
    main()
