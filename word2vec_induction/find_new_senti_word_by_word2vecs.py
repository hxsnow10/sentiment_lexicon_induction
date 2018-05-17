# encoding=utf-8
import time
from multiprocessing import Pool
from copy import deepcopy
from collections import defaultdict
import os
import re

import argparse
from gensim.models import word2vec
import numpy
from numpy import ndarray
import numpy as np

import sys
reload(sys)
sys.setdefaultencoding("utf-8")
filter=re.compile(u'[0-9１２３４５６７８９０@、，？。a-zA-Z]', re.UNICODE)

def filter_word(word):
    return filter.search(word.decode('utf-8'))

def read_vocab_count(vocab_path):
    word_count={}
    if vocab_path: 
        ii=open(vocab_path)
        for line in ii:
            try:
                w,c=line.strip().split()
                word_count[w]=int(c)
            except Exception:
                print line
    return word_count 

def ratio(word_count):
    s=sum(word_count.values())
    for w in word_count:
        word_count[w]=1.0*word_count[w]/s
    return word_count

def main(senti_dict_path, vec_paths, vocab_paths, output_dir, deepth, r, topn=200, th=0.5,
        action='domain_seed'):
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    mean_weight, weight = get_relative_scores(vocab_paths)
    final_rval = {}
    count=defaultdict(int)
    for vec_path in vec_paths:
        rval = get_expand_results(senti_dict_path, vec_path)
        for word,mode,original_senti,new_senti,probs,supports in rval:
            if word not in rval:
                final_rval[word]=[mode,original_senti,new_senti,[probs],supports]
            elif new_senti!=confilcts[word][1]:
                count[word]+=1
                final_rval["{}_{}".format(word,count[word])]=[mode,original_senti,new_senti,[probs],supports]
            else:
                final_rval[word][3].append(probs)
                final_rval[word][4]+=supports
    senti_dict=read_senti_dict(senti_dict_path)
    yiyuan_senti_dict=read_senti_dict('yiyuan.txt')
    for word in senti_dict:
        word=word.encode('utf-8')
        if word not in final_rval:
            final_rval[word]=['keep',senti_dict[word.decode('utf-8')],'',[],'']
    oo = open(os.path.join(output_dir,'results.txt'),'w')
    if not vocab_paths:vocab_paths=['']*len(vec_paths)
    attrs=['word', 'mode', 'original_senti', 'new_senti', 'prob', 'supports', 'mean_weight']+\
        [vocab_paths]
    for word in final_rval:
        mode,original_senti,new_senti,probs,supports = final_rval[word]
        oo.write('{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\n'.format(
                    word,
                    mode,
                    original_senti, 
                    new_senti,
                    ','.join([str(x) for x in probs]),
                    ','.join(supports),
                    str(mean_weight.get(word,0)),
                    '\t'.join([str(x) for x in weight.get(word,[])]),
                    yiyuan_senti_dict.get(word.decode('utf-8'),'')
                    ))

def get_relative_scores(vocab_paths):
    if not vocab_paths:return {}, {}
    sums=defaultdict(int)
    word_counts = [read_vocab_count(vocab_path) for vocab_path in vocab_paths]
    for word_count in word_counts:
        for word in word_count:
            sums[word]+=word_count[word]
    word_scores = [ratio(word_count) for word_count in word_counts]
    mean_weight, weight=sums, {}
    for w in word_scores[0]:
        scores=[word_score[w] for word_score in word_scores if w in word_score]
        if len(scores)!=len(word_scores):continue
        mean=1.0*sum(scores)/len(scores)
        scores=[1.0*(score-mean)/mean for score in scores]
        weight[w]=scores
    return mean_weight, weight
def read_senti_dict(senti_dict_path):
    ii=open(senti_dict_path,'r')
    senti_dict={}
    for i in ii:
        try:
            w,p=i.strip().split()
            if p not in ['0','1','-1']:continue
            senti_dict[w.decode('utf-8')]=p
        except:
            print i
    ii.close()
    return senti_dict

def get_expand_results(senti_dict_path, vec_path, deepth=1, r=0.8, topn=200, th=0.5,
        action='domain_seed'):
    senti_dict=read_senti_dict(senti_dict_path)
    # read word vectors
    start=time.time()
    model=word2vec.Word2Vec.load_word2vec_format(vec_path,binary=False, unicode_errors='replace')
    print time.time()-start

    # build top similar first
    def zero():
        return 1.0*numpy.array([0,0,0])
    new_d={w:[1.0*numpy.array([0,0,0]),defaultdict(zero)] for w in model.vocab}
    
    def get_score(label):
        a,b=1.0*numpy.array([0,0,0]),defaultdict(zero)
        if label=='0':
            a=1.0*numpy.array([0,1,0])
            b[w]=1.0*numpy.array([0,1,0])
        elif label=='1':
            a=1.0*numpy.array([0,0,1])
            b[w]=1.0*numpy.array([0,0,1])
        elif label=='-1':
            a=1.0*numpy.array([1,0,0])
            b[w]=1.0*numpy.array([1,0,0])
        return a,b

    for w in senti_dict:
        if w in new_d:
            p,s=get_score(senti_dict[w])
            new_d[w]=[p,s]
    
    # 传播来提升每个词的情感分布
    while deepth>0:
        new_dd=deepcopy(new_d)
        r=r*0.8
        for w in new_d:
            if  (new_d[w][0]!=numpy.array([0,0,0])).any():
                similar=model.most_similar(w, topn=topn)
                for ww,p in similar:
                    if filter_word(ww):continue
                    if p>=th:
                        scale=p*p*r
                        new_dd[ww][0]=new_dd[ww][0]+scale*new_d[w][0]
                        for k,v in new_d[w][1].iteritems():
                            new_dd[ww][1][k]+=scale*v
                    else:break
        new_d=new_dd
        deepth-=1

    # give result
    rval=[]
    for w in new_d:
        p,s=new_d[w]
        if (p!=numpy.array([0,0,0])).any():
            p=p/(p.sum()+0.0001)
            index=ndarray.argmax(p)
            supports=[ww.encode('utf-8') for ww,vv in s.iteritems() if ndarray.argmax(vv)==index]
            #supports=','.join(supports)
            probs=p[index]
            if w in senti_dict: 
                if str(index-1)!=senti_dict[w]:
                    rval.append((w.encode('utf-8'), 'conflict', senti_dict[w], str(index-1), str(probs), supports)) 
                else:
                    rval.append((w.encode('utf-8'), 'keep', senti_dict[w], str(index-1), str(probs), supports)) 
                    
            else:
                if str(index-1)=='0':continue
                rval.append((w.encode('utf-8'), 'new', '', str(index-1), str(probs), supports))
    return rval

if __name__=="__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-s', '--senti_dict_path', default='senti_dict.txt', help="senti dict path")
    parser.add_argument('-v', '--vec_paths', nargs='+', help="word vecs paths")
    parser.add_argument('-o', '--output_dir', help="out dir")
    parser.add_argument('-vn', '--vocab_paths', nargs='+', help="word vocab count paths")
    parser.add_argument('-d', '--deepth', type=int, default=1)
    parser.add_argument('-r', '--ratio', type=float, default=0.8)
    parser.add_argument('--topn', type=int, default=200)
    parser.add_argument('--th', type=float, default=0.5)
    args = parser.parse_args()
    main(args.senti_dict_path, args.vec_paths, args.vocab_paths, args.output_dir,\
        args.deepth, args.ratio, args.topn, args.th)
