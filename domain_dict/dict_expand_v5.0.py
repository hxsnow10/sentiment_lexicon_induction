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
expand_dict_file = "expand_dict_v5.0.txt"

import re
import time
import math

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
    total_sub_sentence = 0
    count_dict = dict()
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
                    subsent_words_set = set()
                    for temp_word in temp_subsent_words:
                        word = temp_word.strip()
                        if word == '':
                            continue
                        elif re.match(r'^\d|^http|^www|.*\d$|.*[、\.%％/-].*',word)!=None:
                            # 过滤数据、网址
                            continue
                        else:
                            subsent_words_set.add(word)
                            
                    if len(subsent_words_set)==0:
                        continue
                    total_sub_sentence += 1
                    
                    subsent_words_candidates = list()
                    present_pos_seeds = list()
                    present_neg_seeds = list()
#==============================================================================
#列表是按下标遍历的，依次让index=0，1，2，3，4，5，6，7，8，9，···，提取元素
#                         a=[0，1,2,3,4,5,6,7,8,9]
#                         for i in a:
#                             print i
#                             a.remove(i)
#                         输出结果: 0 2 4 6 8
#如果在遍历列表的时候，各元素的下标是变动的，那么遍历就会出问题，有可能遗漏元素!!!
#==============================================================================
                    for word in subsent_words_set:
                        if word in count_dict:
                            count_dict[word] += 1
                        else:
                            count_dict[word] = 1
                        if word in pos_seeds_list:
                            present_pos_seeds.append(word)
                            continue
                        if word in neg_seeds_list:
                            present_neg_seeds.append(word)
                            continue
                        if len(word.decode('utf-8'))>1:
                            # len('我')=3
                            # len('我'.decode('utf-8'))=1
                            # 过滤单字及标点符号
                            subsent_words_candidates.append(word)
                    
                    if len(present_pos_seeds)>0:
                        for word in subsent_words_candidates:
                            if word not in pos_candidates:
                                pos_candidates[word] = dict()
                                for pos_seed in present_pos_seeds:
                                    pos_candidates[word][pos_seed] = 1
                            else:
                                for pos_seed in present_pos_seeds:
                                    if pos_seed not in pos_candidates[word]:
                                        pos_candidates[word][pos_seed] = 1
                                    else:
                                        pos_candidates[word][pos_seed] += 1
                    if len(present_neg_seeds)>0:
                        for word in subsent_words_candidates:
                            if word not in neg_candidates:
                                neg_candidates[word] = dict()
                                for neg_seed in present_neg_seeds:
                                    neg_candidates[word][neg_seed] = 1
                            else:
                                for neg_seed in present_neg_seeds:
                                    if neg_seed not in neg_candidates[word]:
                                        neg_candidates[word][neg_seed] = 1
                                    else:
                                        neg_candidates[word][neg_seed] += 1

        fr.close()
    print("-------------------------获得候选词-----------------------------")
    return total_sub_sentence,count_dict,pos_candidates,neg_candidates
    
#在候选词中挑选出真正可靠的pos词和neg词
def pick_authentic_dict(total_sub_sentence,count_dict,pos_candidates,neg_candidates,dict_file):
    print("-------------------------筛选候选词-----------------------------")
    mutual_info_with_pos = dict()
    for word,present_seeds_dict in pos_candidates.items():
        mutual_info_sum = 0
        for pos_seed,co_occur_counts in present_seeds_dict.items():
            mutual_info = math.log( (co_occur_counts*total_sub_sentence*1.0)/(count_dict[word]*count_dict[pos_seed]) )
            percent = co_occur_counts*1.0/total_sub_sentence
            weight = 1.0/math.log(1.0/percent)
            mutual_info_sum += weight*mutual_info
        mutual_info_with_pos[word] = mutual_info_sum
    
    mutual_info_with_neg = dict()
    for word,present_seeds_dict in neg_candidates.items():
        total_co_occur = 0
        for co_occur_counts in present_seeds_dict.values():
            total_co_occur += co_occur_counts
        mutual_info_sum = 0
        for neg_seed,co_occur_counts in present_seeds_dict.items():
            mutual_info = math.log( (co_occur_counts*total_sub_sentence*1.0)/(count_dict[word]*count_dict[neg_seed]) )
            percent = co_occur_counts*1.0/total_sub_sentence
            weight = 1.0/math.log(1.0/percent)
            mutual_info_sum += weight*mutual_info
        mutual_info_with_neg[word] = mutual_info_sum
    
    alpha = 3
    beta = 1.5
    for word in mutual_info_with_pos.keys():
        if word in mutual_info_with_neg:
            if mutual_info_with_pos[word]>alpha*mutual_info_with_neg[word]:
                mutual_info_with_neg[word] = 0
            elif mutual_info_with_neg[word]>beta*mutual_info_with_pos[word]:
                mutual_info_with_pos[word] = 0
            else:
                mutual_info_with_pos[word] = 0
                mutual_info_with_neg[word] = 0

    print("-------------------------筛选完毕------------------------------")
    print("-------------------------开始排序------------------------------")
    sorted_pos = sorted(mutual_info_with_pos.items(), key=lambda d:d[1], reverse=True)
    sorted_neg = sorted(mutual_info_with_neg.items(), key=lambda d:d[1], reverse=False) 
    print("-------------------------排序完毕------------------------------")
    print("-------------------------写入文件------------------------------")
    with open(dict_file,'w') as fw:
        if len(sorted_pos)>0:
            for word,mutual_info_sum in sorted_pos:
                if mutual_info_sum > 0:
                    score = round(mutual_info_sum, 5)
                    fw.write(word.strip() + "\t" + str(score) + "\n")
        if len(sorted_neg)>0:
            for word,mutual_info_sum in sorted_neg:
                if mutual_info_sum > 0:
                    score = round(-mutual_info_sum, 5)
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
    total_sub_sentence,count_dict,pos_candidates,neg_candidates = expand_candidates_dict(args_tuple)
    #在候选词中挑选出真正可靠的pos词和neg词，并写入文件
    pick_authentic_dict(total_sub_sentence,count_dict,pos_candidates,neg_candidates,expand_dict_file)
    
    end_time = time.time()
    delta_minutes = round((end_time-start_time)/60, 1)
    print("It takes %s minutes."%str(delta_minutes) )
    
    print("Done!")
