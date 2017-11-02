# -*- coding: utf-8 -*-
"""
Created on Thu Sep 14 09:59:33 2017

@author: hukai
"""
deny_words_file = "deny_words.txt" #否定词文件
transition_words_file = "transition_words.txt" #转折词文件
subjunctive_words_file = "subjunctive_words.txt" #虚拟词文件
seeds_dict_file = "senti_dict.txt" #种子词典
Hownet_poswords_file = "Hownet_zh_pos.txt"
Hownet_negwords_file = "Hownet_zh_neg.txt"
segmented_corpus = "../segment_words/seg_result_text_new.txt"
expand_dict_file = "expand_dict_v5.2.txt"
pickle_file = "data.pickle"
processes = 30 #候选词拓展子进程的数量
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

def build_words_list(words_file):
    '''
    创建必要的词列表:否定词、转折词、虚拟词
    '''
    words_list = list()
    with open(words_file, "r") as fr:
        for line in fr.readlines():
            word = line.strip()
            if word not in words_list:
                words_list.append(word)
        fr.close()
    return words_list

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
            if score == '-1':   
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

def read_segmented_text(segmented_corpus_file, segmented_text_queue, segmented_text_state):
    #用with打开文件，如果文件打不开，或者with块内出现错误，程序出错但不报错
    #折腾半天不知道程序原来错在这里
    with open(segmented_corpus_file, 'r') as fr:
        while True:
            text_list = list()
            count = 0
            #切分的文本数据的本地保存方式，一行一篇文本
            for line_text in fr:
                text_list.append(line_text)
                count += 1
                if count >= 100: break
            if count==0:break
            #put()方法在队尾插入一个项目
            #如果队列已满，put()方法就使子进程阻塞,直到空出一个数据单元
            segmented_text_queue.put(text_list, block=True, timeout=None)
            print('Put a list of segmented texts to queue!')
            sys.stdout.flush()
        fr.close()
    segmented_text_state.set("empty")
    print('Putting segmented texts into queue is finished!')
    sys.stdout.flush()
    return

def expand_candidates_dict(args_tuple):
    """
    利用词与种子词的共现关系
    """
    segmented_text_state, segmented_text_queue, candidates_queue, pos_seeds_list, neg_seeds_list = args_tuple
    while (not segmented_text_queue.empty()) or segmented_text_state.value=='have':
        #get()方法从队头删除并返回一个项目
        #如果队列为空，get()方法就使进程阻塞timeout秒。
        #如果在timeout秒内，发现可用的项目，则继续执行。如果超时，则引发一个异常。
        try:
            segmented_text_list = segmented_text_queue.get(block=True, timeout=0.1)
        except: continue 
        print('Get a list of segmented texts from queue! Analyze it now!')
        sys.stdout.flush()
        #-------------------------开始分析-----------------------------
        sub_sentence_num = 0
        count_dict = dict()
        pos_candidates = dict()  #拓展出的积极候选词
        neg_candidates = dict()  #拓展出的消极候选词
        for text in segmented_text_list:
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
                    sub_sentence_num += 1
                    
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

        #-------------------------获得候选词-----------------------------
        #一个文本列表(100篇文章)拓展出的候选词
        candidates_dict_tuple = (sub_sentence_num, count_dict, pos_candidates, neg_candidates)
        #put()方法在队尾插入一个项目
        #如果队列已满，put()方法就使子进程阻塞,直到空出一个数据单元
        candidates_queue.put(candidates_dict_tuple, block=True, timeout=None)
        print('Put a tuple of candidates dict to queue!')
        sys.stdout.flush()
    return
    
#在候选词中挑选出真正可靠的pos词和neg词
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
        manager = multiprocessing.Manager()
        segmented_text_queue = manager.Queue(maxsize=Queue_SIZE) #创建分词文本队列
        #设置分词文本状态标记，用来表示分词的文本是否读入完毕。segmented_text_state是一个对象。
        segmented_text_state = manager.Value(None,'have')
        candidates_queue = manager.Queue(maxsize=Queue_SIZE) #创建候选词队列    
        
        #创建进程池
        pool=multiprocessing.Pool(processes = args.processes)
        #创建一个读进程,将已经分词的文本读入分词文本队列。主进程不阻塞
        #python中函数传递参数：若参数是值类型则是值传递，如int、str；若参数是对象类型则是引用传递，如对象、list。
        #segmented_text_state是一个对象，因此是引用传递。当引用的函数修改它时，主函数中它会相应改变，其他引用的函数中它也会相应改变。
        #因此可以用segmented_text_sign作为全局的标记。
        pool.apply_async(read_segmented_text, (segmented_corpus, segmented_text_queue, segmented_text_state))
        #pool.apply()为阻塞版本，主进程会被阻塞直到子进程执行结束
        
        #创建一个筛选候选词的进程，对所有的候选词进行统计分析，并将筛选结果写入文件。主进程不阻塞
        selector = multiprocessing.Process(target=gather_dict, args=(candidates_queue, pickle_file))
        selector.start() #启动写者进程
        
        #创建args.processes-1个拓展词典的进程，执行函数为expand_candidates_dict(args_tuple)。主进程不阻塞
        args_tuple = (segmented_text_state, segmented_text_queue, candidates_queue, pos_seeds_list, neg_seeds_list)
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
        selector.join() #等待筛选者进程结束
        
    
    #加载pickle数据文件，并输出筛选的扩展词典
    pick_authentic_dict(pickle_file,expand_dict_file)
    
    end_time = time.time()
    delta_minutes = round((end_time-start_time)/60, 1)
    print("It takes %s minutes."%str(delta_minutes) )

    print("Done!")
    