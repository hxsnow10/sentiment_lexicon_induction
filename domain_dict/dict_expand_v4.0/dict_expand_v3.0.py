# -*- coding: utf-8 -*-
"""
Created on Thu Sep 14 09:59:33 2017

@author: hukai
"""

deny_words_file = "deny_words.txt" #否定词文件
transition_words_file = "transition_words.txt" #转折词文件
subjunctive_words_file = "subjunctive_words.txt" #虚拟词文件
seeds_dict_file = "seeds_dict.txt" #种子词典
Hownet_poswords_file = "Hownet_zh_pos.txt"
Hownet_negwords_file = "Hownet_zh_neg.txt"
segmented_corpus = "../segment_words/seg_result_text1.txt"
expand_dict_file = "expand_dict_v3.0.txt"

import re
import time

def seedwords_to_list(seeds_file):
    """
    把种子词典导入相应的pos词列表和neg词列表
    """
    pos_seeds_list = list()
    neg_seeds_list = list()
    with open(seeds_file, "r") as fr:
        for line in fr.readlines():
            word,score = line.split()
            if score == '1':
                if word not in pos_seeds_list:
                    pos_seeds_list.append(word)
            else:    
                if word not in neg_seeds_list:
                    neg_seeds_list.append(word)
        fr.close()
    return pos_seeds_list, neg_seeds_list

def hownet_to_list(poswords_file, negwords_file):
    """
    把知网词库导入到相应的pos词列表和neg词列表
    """
    poswords_list = list()
    negwords_list = list()
    common_words = set()  #公共词集
    with open(poswords_file, 'r') as fpos, open(negwords_file, 'r') as fneg:
        #读取Hownet的pos词
        for line in fpos.readlines():
            word = line.strip()
            if word not in poswords_list:
                poswords_list.append(word)
        #读取Hownet的neg词，若这个词同时属于poswords_list，则不录入negwords_list，而是录入公共词集
        for line in fneg.readlines(): 
            word = line.strip()
            if word in poswords_list:
                common_words.add(word)
                continue
            elif word not in negwords_list:
                negwords_list.append(word)
        #从poswords_list中去掉公共词集中的词
        for com in common_words:
            if com in poswords_list:
                del( poswords_list[poswords_list.index(com)] )
        fpos.close()
        fneg.close()
    return poswords_list, negwords_list

def expand_candidates_dict(args_tuple):
    """
    利用词与种子词的共现关系
    """
    pos_seeds_list, neg_seeds_list, segment_corpus = args_tuple
    #deny_win = 3  #查找否定词的窗口大小，向前查找
    pos_candidates = dict()  #拓展出的积极候选词
    neg_candidates = dict()  #拓展出的消极候选词
    print("-------------------------开始分析-------------------------------")
    with open(segment_corpus,'r') as fr:
        for text in fr:
            #根据句末标点把文本分句
            sentence_list = re.split('。|!|！',text)
            for sentence in sentence_list:
                #针对出现连续标点的情况，如'a。!b' ---> ['a','','b']，标点之间有空格或者无空格
                if sentence.strip() == '': continue
                #一句话包括多个子句，划分子句
                sub_sentence_list = re.split(',|，|;|；|\\?|？', sentence)
                for sub_sentence in sub_sentence_list:
                    #针对出现连续标点的情况，如'a,;b' ---> ['a','','b']，标点之间有空格或者无空格
                    if sub_sentence.strip() == '': continue
                    temp_subsent_words = sub_sentence.split('|')
                    subsent_words = list()
                    for temp_word in temp_subsent_words:
                        word = temp_word.strip()
                        if word == '':
                            continue
                        elif len(word.decode('utf-8'))<=1:
                            # len('我')=3
                            # len('我'.decode('utf-8'))=1
                            # 过滤单字及标点符号
                            continue
                        elif re.match(r'^\d|^http|^www|.*\d$|.*[、,，\.。%％/-].*',word)!=None:
                            # 过滤数据、网址
                            continue
                        else:
                            subsent_words.append(word)
                            
                    if len(subsent_words)==0:
                        continue
                    
                    for pos in pos_seeds_list:
                        if pos in subsent_words:
                            for word in subsent_words:
                                if word in pos_candidates:
                                    pos_candidates[word] += 1
                                else:
                                    pos_candidates[word] = 1
                    for neg in neg_seeds_list:
                        if neg in subsent_words:
                            for word in subsent_words:
                                if word in neg_candidates:
                                    neg_candidates[word] += 1
                                else:
                                    neg_candidates[word] = 1
        fr.close()
    print("-------------------------获得候选词-----------------------------")
    return pos_candidates,neg_candidates
    
#在候选词中挑选出真正可靠的pos词和neg词
def pick_authentic_dict(pos_candidates,neg_candidates,dict_file):
    print("-------------------------筛选候选词-----------------------------")
    picked_pos_dict = dict()
    picked_neg_dict = dict()
    alpha = 5
    for word in pos_candidates.keys():
        if word not in neg_candidates:
            picked_pos_dict[word] = 1
        elif pos_candidates[word]>alpha*neg_candidates[word]:
            picked_pos_dict[word] = pos_candidates[word]*2.0/(pos_candidates[word]+alpha*neg_candidates[word])-1
    beta = 3
    for word in neg_candidates.keys():
        if word not in pos_candidates:
            picked_neg_dict[word] = 1
        elif neg_candidates[word]>beta*pos_candidates[word]:
            picked_neg_dict[word] = neg_candidates[word]*2.0/(neg_candidates[word]+beta*pos_candidates[word])-1

    print("-------------------------筛选完毕------------------------------")
    print("-------------------------开始排序------------------------------")
    sorted_pos = sorted(picked_pos_dict.items(), key=lambda d:d[1], reverse=True)
    sorted_neg = sorted(picked_neg_dict.items(), key=lambda d:d[1], reverse=False) 
    print("-------------------------排序完毕------------------------------")
    print("-------------------------写入文件------------------------------")
    with open(dict_file,'w') as fw:
        for word,score in sorted_pos:
            score = round(score, 5)
            fw.write(word + "\t" + str(score) + "\n")
        for word,score in sorted_neg:
            score = round(-score, 5)
            fw.write(word + "\t" + str(score) + "\n")
        fw.flush()
        fw.close()
    print("-------------------------写入完毕------------------------------")
    return
    
if __name__ == '__main__':
    start_time = time.time()
 
    #把种子词典导入相应的pos词列表和neg词列表
    pos_seeds_list, neg_seeds_list = seedwords_to_list(seeds_dict_file)

    #利用语料拓展词典
    args_tuple = ( pos_seeds_list, neg_seeds_list, segmented_corpus)
    pos_candidates,neg_candidates = expand_candidates_dict(args_tuple)
    #在候选词中挑选出真正可靠的pos词和neg词，并写入文件
    pick_authentic_dict(pos_candidates,neg_candidates,expand_dict_file)
    
    end_time = time.time()
    delta_minutes = round((end_time-start_time)/60, 1)
    print("It takes %s minutes."%str(delta_minutes) )
    
    print("Done!")
