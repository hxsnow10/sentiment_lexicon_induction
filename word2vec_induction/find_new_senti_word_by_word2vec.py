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
filter=re.compile(u'[0-9１２３４５６７８９０@、？。a-zA-Z]', re.UNICODE)
def filter_word(word):
    return filter.search(word.decode('utf-8'))

all_senti_dict={}
for i in open('senti_dict.txt','r'):
    w,p=i.strip().split()
    if p not in ['0','1','-1']:continue
    all_senti_dict[w.decode('utf-8')]=p

def main(senti_dict_path, vec_path, output_dir, deepth, r, topn=200, th=0.5, vocab_path=None,
        action='domain_seed'):
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    ii=open(senti_dict_path,'r')
    senti_dict={}
    for i in ii:
        w,p=i.strip().split()
        if p not in ['0','1','-1']:continue
        senti_dict[w.decode('utf-8')]=p
    ii.close()
    
    # read word count
    word_count={}
    if vocab_path: 
        ii=open(vocab_path)
        for line in ii:
            try:
                w,c=line.strip().split()
                word_count[w]=c
            except Exception:
                print line
    oo=open(os.path.join(output_dir,"original_senti_dict.txt"),'w')
    for word,p in senti_dict.iteritems():
        oo.write(word.encode('utf-8')+'\t'+p+'\t'+str(word_count.get(word.encode('utf-8'),10))+'\n')

    # read word vectors
    start=time.time()
    model=word2vec.Word2Vec.load_word2vec_format(vec_path,binary=False, unicode_errors='replace')
    print time.time()-start


    # build top similar first
    '''
    most_similar={}
    words=model.vocab.keys()
    def f(x):
        return model.most_similar(positive=[x])
    pool=Pool(20)
    sims=pool.map(f,words)
    for w,sim in zip(words,sims):
        most_similar[w]=sim
    '''
    # build source dict
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
    result=[[],[],[]]
    conflict=[]
    for w in new_d:
        p,s=new_d[w]
        if (p!=numpy.array([0,0,0])).any():
            p=p/(p.sum()+0.0001)
            index=ndarray.argmax(p)
            supports=[ww.encode('utf-8') for ww,vv in s.iteritems() if ndarray.argmax(vv)==index]
            supports=','.join(supports)
            probs=p[index]
            if w in senti_dict: 
                if str(index-1)!=senti_dict[w]:
                    conflict.append((w.encode('utf-8'), senti_dict[w], str(index-1), str(probs), supports))
                    
            else: 
                result[index].append((w.encode('utf-8'),str(probs),supports))

    # print conflict word 
    oo=open(os.path.join(output_dir,'conflict.txt'),'w')
    oo.write('\t'.join(['word','orginal','new','probs'])+'\n')
    conflict=sorted(conflict, key=lambda x:(x[2],float(x[3])),reverse=True)
    for i in conflict:
        oo.write('\t'.join(i)+'\n')
    oo.close()

    # print senti result
    print 'FIND NUMS FOR SENTI=-1,0,1:', [len(i) for i in result]
    for i in range(3):
        result[i]=sorted(result[i], key=lambda x:(x[1],x[0]), reverse=True)
        oo=open(os.path.join(output_dir, 'result'+str(i)+'.txt'),'w')
        for w,p,s in result[i]:
            oo.write(w+'\t'+str(all_senti_dict.get(w.decode('utf-8'),None))+'\t'+\
                    str(p)+'\t'+str(s)+'\t'+word_count.get(w,'10')+'\n')
        oo.close()

if __name__=="__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-s', '--senti_dict_path')
    parser.add_argument('-v', '--vec_path')
    parser.add_argument('-o', '--output_dir')
    parser.add_argument('-vn', '--vocab_path', default=None)
    parser.add_argument('-d', '--deepth', type=int, default=1)
    parser.add_argument('-r', '--ratio', type=float, default=0.8)
    parser.add_argument('--topn', type=int, default=2000)
    parser.add_argument('--th', type=float, default=0.5)
    parser.add_argument('--limited',default='limited')
    args = parser.parse_args()
    main(args.senti_dict_path, args.vec_path, args.output_dir,\
        args.deepth, args.ratio, args.topn, args.th, args.vocab_path)
