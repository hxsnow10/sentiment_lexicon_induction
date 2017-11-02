# -*- coding: utf-8 -*-
"""
Created on Thu Oct 19 11:29:13 2017

@author: hukai
"""

import os
import json
import time
import re
import math

segment_result_file = "seg_result_text1.txt"
sorted_words_file = "sorted_words_using_TF_IDF_v1.1.txt"
temp_TF_dict_file = "temp_TF_dict.txt"

#计算词条频率
def TF(words_list):
    temp_TF_dict = dict()
    for temp_word in words_list:
        word = temp_word.strip()
        # len('我')=3
        # len('我'.decode('utf-8'))=1
        # 过滤单字及标点符号
        if len(word.decode('utf-8'))<=1: continue
        # 过滤数据、网址
        if re.match(r'^\d|^http|^www|.*\d$|.*[、,，\.。%％/-\\”“"‘’\'].*',word)!=None: continue
        if word in temp_TF_dict:
            temp_TF_dict[word] += 1
        else:
            temp_TF_dict[word] = 1
    text_length = len(words_list)
    TF_dict = { word:count*1.0/text_length for word,count in temp_TF_dict.iteritems() }
    return TF_dict

def IDF(TF_dict_file):
    fr = open(TF_dict_file,'r')
    total_texts = 0
    temp_IDF_dict = dict()
    for line in fr:
        total_texts += 1
        TF_dict = json.loads(line.strip())
        for word in TF_dict.keys():
            if word in temp_IDF_dict:
                temp_IDF_dict[word] += 1
            else:
                temp_IDF_dict[word] = 1
    fr.close()
    IDF_dict = dict()
    for word,count in temp_IDF_dict.items():
        #过滤出现次数很少的分词，这种词往往算不上词汇，比如只在一篇文章中出现的数据、人名
        if count>10:
            IDF_dict[word] = math.log10(total_texts*1.0/count)
    return IDF_dict

def sort_words_using_TF_IDF(TF_dict_file,IDF_dict,save_TFIDF_words_file):
    fr = open(TF_dict_file,'r')
    fw = open(save_TFIDF_words_file,'w')
    for line in fr:
        TF_dict = json.loads(line.strip())
        TF_IDF_dict = dict()
        for word,word_freq in TF_dict.items():
            if word in IDF_dict:
                TF_IDF_dict[word] = word_freq*IDF_dict[word]
        sorted_words_list = sorted(TF_IDF_dict.items(), key=lambda item:item[1], reverse=True)
        words_str = ''
        if len(sorted_words_list)>10:
            for word,TF_IDF_value in sorted_words_list[:-2]:
                words_str += word.encode('utf-8') + ' '
            words_str += sorted_words_list[-1][0].encode('utf-8')
            fw.write(words_str+"\n")
    fr.close()
    fw.flush()
    fw.close()
    return

if __name__ == '__main__':
    start_time = time.time()
    
    fr = open(segment_result_file,'r')
    fw = open(temp_TF_dict_file,'w')
    for line in fr:
        words_list = line.split('|')
        TF_dict = TF(words_list)
        dict_str = json.dumps(TF_dict)
        fw.write(dict_str+"\n")
    fr.close()
    fw.flush()
    fw.close()
    IDF_dict = IDF(temp_TF_dict_file)
    sort_words_using_TF_IDF(temp_TF_dict_file,IDF_dict,sorted_words_file)
    os.remove(temp_TF_dict_file)
    
    end_time = time.time()
    delta_minutes = round((end_time-start_time)/60, 1)
    print("It takes %s minutes."%str(delta_minutes) )
    
    print("Done!")
