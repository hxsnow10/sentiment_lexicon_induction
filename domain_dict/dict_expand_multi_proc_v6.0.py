# -*- coding: utf-8 -*-
"""
Created on Thu Sep 14 09:59:33 2017

@author: hukai
"""

seeds_dict_file = "new_seeds.txt" #种子词典
existed_dict = "senti_dict.txt"
tag_corpus = "../segment_words/tag_食品餐饮.txt"
expand_dict_file = "expand_dict_v6.0.txt"
pickle_file = "data.pickle.v6.0"
processes = 10 #候选词拓展子进程的数量
Queue_SIZE = 100 #共享队列能够容纳的元素个数

import argparse
import re
import sys
import multiprocessing
import time
import math
import pickle

#获得初始输入参数
def get_initial_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('-p', dest='processes', type=int, default=processes, help="Set the process number. Must greater than 1.")
    parser.add_argument('--pickle', type=int, default=0, help="Whether load pickle data directly or not? Expressed by 1 or 0.")
    args = parser.parse_args()
    #若输入的参数不满足要求则退出程序
    #如果进程数小于3，程序就不能一边读入分词文本，一边处理分词文本，一边统计结果
    if args.processes<2:
        sys.exit("The process number should be greater than 1.")
    if args.pickle!=0 and args.pickle!=1:
        sys.exit("The pickle number should be 0 or 1.")
    return args

def seedwords_to_list(seeds_file):
    """
    把种子词典导入相应的pos词列表和neg词列表
    """
    pos_seeds_list = list()
    neg_seeds_list = list()
    with open(seeds_file, "r") as fr:
        for line in fr.readlines():
            temp_list = line.split()
            word = temp_list[0]
            score = temp_list[1]
            if score == '1':
                if word not in pos_seeds_list:
                    pos_seeds_list.append(word)
            if score == '-1':   
                if word not in neg_seeds_list:
                    neg_seeds_list.append(word)
        fr.close()
    return pos_seeds_list, neg_seeds_list

def existed_dict_load(dict_file):
    """
    把存在的词典加载
    """
    existed_dict = dict()
    with open(dict_file, "r") as fr:
        for line in fr.readlines():
            temp_list = line.split()
            word = temp_list[0]
            score = temp_list[1]
            if word not in existed_dict:
                existed_dict[word] = score
        fr.close()
    return existed_dict

def read_text(corpus_file, text_queue, text_state):
    #用with打开文件，如果文件打不开，或者with块内出现错误，程序出错但不报错
    #折腾半天不知道程序原来错在这里
    with open(corpus_file, 'r') as fr:
        while True:
            text_list = list()
            count = 0
            #切分的文本数据的本地保存方式，一行一篇文本
            for line_text in fr:
                text_list.append(line_text.strip())
                count += 1
                if count >= 100: break
            if count==0:break
            #put()方法在队尾插入一个项目
            #如果队列已满，put()方法就使子进程阻塞,直到空出一个数据单元
            text_queue.put(text_list, block=True, timeout=None)
            print('Put a list of tagged texts to queue!')
            sys.stdout.flush()
        fr.close()
    text_state.set("empty")
    print('Putting tagged texts into queue is finished!')
    sys.stdout.flush()
    return

def expand_candidates_dict(args_tuple):
    """
    利用特定词性的词与种子词的共现关系
    """
    tag_text_state, tag_text_queue, candidates_queue, pos_seeds_list, neg_seeds_list,existed_senti_dict = args_tuple
    while (not tag_text_queue.empty()) or tag_text_state.value=='have':
        #get()方法从队头删除并返回一个项目
        #如果队列为空，get()方法就使进程阻塞timeout秒。
        #如果在timeout秒内，发现可用的项目，则继续执行。如果超时，则引发一个异常。
        try:
            tag_text_list = tag_text_queue.get(block=True, timeout=0.1)
        except: continue 
        print('Get a list of tagged texts from queue! Analyze it now!')
        sys.stdout.flush()
        #-------------------------开始分析-----------------------------
        sub_sentence_num = 0
        count_dict = dict()
        pos_candidates = dict()  #拓展出的积极候选词
        neg_candidates = dict()  #拓展出的消极候选词
        for text in tag_text_list:
            #根据句末标点把文本分句
            sentence_list = re.split('。/wp|!/wp|！/wp',text)
            for sentence in sentence_list:
                #针对出现连续标点的情况，如'a。!b' ---> ['a','','b']，标点之间有空格或者无空格
                if sentence.strip() == '': continue
                #一句话包括多个子句，划分子句
                sub_sentence_list = re.split(',/wp|，/wp|;/wp|；/wp|\\?/wp|？/wp', sentence)
                for sub_sentence in sub_sentence_list:
                    #针对出现连续标点的情况，如'a,;b' ---> ['a','','b']，标点之间有空格或者无空格
                    if sub_sentence.strip() == '': continue
                    word_tag_list = sub_sentence.split()
                    word_tag_dict = dict()
                    for word_tag in word_tag_list:
                        temp_list = word_tag.split('/')
                        word = ''.join(temp_list[:-1])
                        tag = temp_list[-1]
                        word_tag_dict[word] = tag
                    
                    if len(word_tag_dict)==0:
                        continue
                    sub_sentence_num += 1
                    
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
                    for word in word_tag_dict.keys():
                        if word in count_dict:
                            count_dict[word] += 1
                        else:
                            count_dict[word] = 1
                        if word in pos_seeds_list:
                            present_pos_seeds.append(word)
                            word_tag_dict.pop(word)
                            continue
                        if word in neg_seeds_list:
                            present_neg_seeds.append(word)
                            word_tag_dict.pop(word)
                            continue
                        if len(word.decode('utf-8'))==1:
                            #len('我')=3
                            #len('我'.decode('utf-8'))=1
                            #过滤单字及标点符号
                            word_tag_dict.pop(word)
                            continue
                        if word in existed_senti_dict:
                            word_tag_dict.pop(word)
                    
                    #拓展什么词性的词作为候选词,'a'形容词，'i'俚语成语，'n'普通名词，'v'动词
                    object_tag = ['a','i','v']
                    if len(present_pos_seeds)>0:
                        for word,tag in word_tag_dict.items():
                            if tag not in object_tag: continue
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
                        for word,tag in word_tag_dict.items():
                            if tag not in object_tag: continue
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

        #-------------------------获得候选词-----------------------------
        #一个文本列表(100篇文章)拓展出的候选词
        candidates_dict_tuple = (sub_sentence_num, count_dict, pos_candidates, neg_candidates)
        #put()方法在队尾插入一个项目
        #如果队列已满，put()方法就使子进程阻塞,直到空出一个数据单元
        candidates_queue.put(candidates_dict_tuple, block=True, timeout=None)
        print('Put a tuple of candidates dict to queue!')
        sys.stdout.flush()
    return

#汇总候选词典
def gather_dict(candidates_queue, pickle_file):
    print('Beginning to gather candidates dict!')
    total_sub_sentence = 0
    gather_count_dict = dict()
    gather_pos_candidates = dict()
    gather_neg_candidates = dict()
    while True:
        #get()方法从队头删除并返回一个项目
        #如果队列为空，get()方法就使进程阻塞,直至有项目可用
        #一个词典拓展进程拓展出的候选词保存为一个元组(pos_candidates, neg_candidates)
        candidates_tuple = candidates_queue.get(block=True, timeout=None)
        #将所有的候选词统计完之后，会取到一个结束标记'over'
        if candidates_tuple == 'over':
            break
        print('Get a tuple of candidates dict from queue!')
        sys.stdout.flush()    
        sub_sentence_num, count_dict, pos_candidates, neg_candidates = candidates_tuple
        #汇总子句的数量
        total_sub_sentence += sub_sentence_num
        #汇总计数词典
        for word,counts in count_dict.items():
            if word not in gather_count_dict:
                gather_count_dict[word] = count_dict[word]
            else:
                gather_count_dict[word] += count_dict[word]
        #汇总拓展出的积极候选词
        for word,pos_seeds_dict in pos_candidates.items():
            if word not in gather_pos_candidates:
                gather_pos_candidates[word] = pos_seeds_dict
            else:
                for pos_seed,count in pos_seeds_dict.items():
                    if pos_seed not in gather_pos_candidates[word]:
                        gather_pos_candidates[word][pos_seed] = pos_seeds_dict[pos_seed]
                    else:
                        gather_pos_candidates[word][pos_seed] += pos_seeds_dict[pos_seed]
        #汇总拓展出的消极候选词
        for word,neg_seeds_dict in neg_candidates.items():
            if word not in gather_neg_candidates:
                gather_neg_candidates[word] = neg_seeds_dict
            else:
                for neg_seed,count in neg_seeds_dict.items():
                    if neg_seed not in gather_neg_candidates[word]:
                        gather_neg_candidates[word][neg_seed] = neg_seeds_dict[neg_seed]
                    else:
                        gather_neg_candidates[word][neg_seed] += neg_seeds_dict[neg_seed]
    print('Gathering candidates dict is finished!')                    
    print("--------------持久化候选词--------------")
    sys.stdout.flush()
    pickle_tuple = (total_sub_sentence,gather_count_dict,gather_pos_candidates,gather_neg_candidates)
    fw = open(pickle_file,'wb')
    pickle.dump(pickle_tuple, fw)
    fw.flush()
    fw.close()
    print("--------------持久化完毕----------------")
    sys.stdout.flush()
    return

#加载pickle数据文件，并输出筛选的扩展词典
def pick_authentic_dict(pickle_file,dict_save_file):
    print("--------------加载候选词----------------")
    sys.stdout.flush()
    try:
        fr = open(pickle_file,'rb')
        pickle_tuple = pickle.load(fr)
        fr.close()
    except Exception:
        print "Pickle file load error!"
        return
    print("--------------加载完毕------------------")
    total_sub_sentence,gather_count_dict,gather_pos_candidates,gather_neg_candidates = pickle_tuple
    print("--------------筛选候选词----------------")
    sys.stdout.flush()
    mutual_info_with_pos = dict()
    for word,present_seeds_dict in gather_pos_candidates.items():
        mutual_info_sum = 0
        for pos_seed,co_occur_counts in present_seeds_dict.items():
            mutual_info = math.log( (co_occur_counts*total_sub_sentence*1.0)/(gather_count_dict[word]*gather_count_dict[pos_seed]) )
            percent = co_occur_counts*1.0/total_sub_sentence
            mutual_info_sum += percent*mutual_info
        mutual_info_with_pos[word] = mutual_info_sum
    
    mutual_info_with_neg = dict()
    for word,present_seeds_dict in gather_neg_candidates.items():
        mutual_info_sum = 0
        for neg_seed,co_occur_counts in present_seeds_dict.items():
            mutual_info = math.log( (co_occur_counts*total_sub_sentence*1.0)/(gather_count_dict[word]*gather_count_dict[neg_seed]) )
            percent = co_occur_counts*1.0/total_sub_sentence
            mutual_info_sum += percent*mutual_info
        mutual_info_with_neg[word] = mutual_info_sum
    
    alpha = 5
    beta = 3
    for word in mutual_info_with_pos.keys():
        if word in mutual_info_with_neg:
            if mutual_info_with_pos[word]>alpha*mutual_info_with_neg[word]:
                mutual_info_with_neg[word] = 0
            elif mutual_info_with_neg[word]>beta*mutual_info_with_pos[word]:
                mutual_info_with_pos[word] = 0
            else:
                mutual_info_with_pos[word] = 0
                mutual_info_with_neg[word] = 0

    print("--------------筛选完毕------------------")
    print("--------------开始排序------------------")
    sys.stdout.flush()
    sorted_pos = sorted(mutual_info_with_pos.items(), key=lambda d:d[1], reverse=True)
    sorted_neg = sorted(mutual_info_with_neg.items(), key=lambda d:d[1], reverse=False) 
    print("--------------排序完毕------------------")
    print("--------------写入文件------------------")
    sys.stdout.flush()
    with open(dict_save_file,'w') as fw:
        if len(sorted_pos)>0:
            for word,mutual_info_sum in sorted_pos:
                score = round(mutual_info_sum, 5)
                if score > 0:
                    present_seeds_dict = gather_pos_candidates[word]
                    sorted_seed = sorted(present_seeds_dict.items(), key=lambda d:d[1], reverse=True)
                    seed_str = sorted_seed[0][0]
                    if len(sorted_seed)>1:
                        for seed_tuple in sorted_seed[1:100]:
                            seed_str += ','+seed_tuple[0]
                    fw.write(word.strip() + "\t" + str(score) + "\t" + seed_str + "\n")
        if len(sorted_neg)>0:
            for word,mutual_info_sum in sorted_neg:
                score = round(-mutual_info_sum, 5)
                if score < 0:
                    present_seeds_dict = gather_neg_candidates[word]
                    sorted_seed = sorted(present_seeds_dict.items(), key=lambda d:d[1], reverse=True)
                    seed_str = sorted_seed[0][0]
                    if len(sorted_seed)>1:
                        for seed_tuple in sorted_seed[1:100]:
                            seed_str += ','+seed_tuple[0]
                    fw.write(word + "\t" + str(score) + "\t" + seed_str + "\n")
        fw.flush()
        fw.close()
    print("--------------写入完毕------------------")
    sys.stdout.flush()
    return
    
if __name__ == '__main__':
    start_time = time.time()
    
    #获得初始输入参数
    args = get_initial_args()
    #是否直接加载pickle数据文件
    if args.pickle == 0:
        #把种子词典导入相应的pos词列表和neg词列表
        pos_seeds_list, neg_seeds_list = seedwords_to_list(seeds_dict_file)
        existed_senti_dict = existed_dict_load(existed_dict)
        manager = multiprocessing.Manager()
        tag_text_queue = manager.Queue(maxsize=Queue_SIZE) #创建标注文本队列
        #设置分词文本状态标记，用来表示分词的文本是否读入完毕。tag_text_state是一个对象。
        tag_text_state = manager.Value(None,'have')
        candidates_queue = manager.Queue(maxsize=Queue_SIZE) #创建候选词队列    
        
        #创建进程池
        pool=multiprocessing.Pool(processes = args.processes)
        #创建一个读进程,将已经分词的文本读入分词文本队列。主进程不阻塞
        #python中函数传递参数：若参数是值类型则是值传递，如int、str；若参数是对象类型则是引用传递，如对象、list。
        #tagged_text_state是一个对象，因此是引用传递。当引用的函数修改它时，主函数中它会相应改变，其他引用的函数中它也会相应改变。
        #因此可以用tagged_text_state作为全局的标记。
        pool.apply_async(read_text, (tag_corpus, tag_text_queue, tag_text_state))
        #pool.apply()为阻塞版本，主进程会被阻塞直到子进程执行结束
        
        #创建一个汇总进程，对所有的候选词进行统计分析，并将筛选结果写入文件。主进程不阻塞
        gatherer = multiprocessing.Process(target=gather_dict, args=(candidates_queue, pickle_file))
        gatherer.start() #启动写者进程
        
        #创建args.processes-1个拓展词典的进程，执行函数为expand_candidates_dict(args_tuple)。主进程不阻塞
        args_tuple = (tag_text_state, tag_text_queue, candidates_queue, pos_seeds_list, neg_seeds_list,existed_senti_dict)
        for i in range(args.processes-1):
            #坑了很久! 在python中，括号()作用有二，表示(元组) 或 优先计算的(算术表达式)
            #如果括号里只有一个元素且不加逗号，python默认括号里是一个算术表达式
            #当加上一个逗号的时候，python解释器会明白这是一个tuple类型，而不是一个算术式。
            pool.apply_async( expand_candidates_dict, (args_tuple,) )
            #pool.apply()为阻塞版本，主进程会被阻塞直到子进程执行结束
        pool.close() #关闭进程池，不再接受新的进程
        pool.join() #等待读进程和词典拓展进程的结束
        print('Putting candidates dict into queue is finished!')
        #词典拓展进程全部结束后，向candidates_queue写入结束标记'over'，用来通知筛选者进程
        candidates_queue.put('over', block=True, timeout=None)
        gatherer.join() #等待筛选者进程结束
        
    
    #加载pickle数据文件，并输出筛选的扩展词典
    pick_authentic_dict(pickle_file,expand_dict_file)
    
    end_time = time.time()
    delta_minutes = round((end_time-start_time)/60, 1)
    print("It takes %s minutes."%str(delta_minutes) )

    print("Done!")
    