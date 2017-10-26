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
segmented_corpus = "../get_text/seg_result_text1.txt"
expand_dict_file = "expand_dict_v1.1.txt"
processes = 5 #候选词拓展子进程的数量
Queue_SIZE = 100 #共享队列能够容纳的元素个数

import argparse
import re
import sys
import multiprocessing
import time

#获得初始输入参数
def get_initial_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('-p', dest='processes', type=int, default=processes, help="Set the process number. Must greater than 1.")
    args = parser.parse_args()
    #若输入的参数不满足要求则退出程序
    #如果进程数小于3，程序就不能一边读入分词文本，一边处理分词文本，一边统计结果
    if args.processes<2:
        sys.exit("The process number should be greater than 1.")
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

def read_segmented_text(segmented_corpus_file, segmented_text_queue , segmented_text_state):
    with open(segmented_corpus_file, 'r') as fr:
        while True:
            text_list = list()
            count = 0
            #切分的文本数据的本地保存方式，一行一篇文本
            for line_text in fr:
                text_list.append(line_text)
                count += 1
                if count >= 5: break
            if count==0:break
            #put()方法在队尾插入一个项目
            #如果队列已满，put()方法就使子进程阻塞,直到空出一个数据单元
            segmented_text_queue.put(text_list, block=True, timeout=None)
        fr.close()
    segmented_text_state.set("empty")
    return

def expand_candidates_dict(args_tuple):
    """
    按自定义规则扩展情感词。规则思想：
    1. 认为每个词有两种极性，pos和neg。
    2. 一句话里不只出现一种极性的词，可以同时出现pos词和neg词。
    3. pos词之间或neg词之间应该是平行关系，pos词和neg词之间应该是对立关系。
    4. 怎么表达这种“对立”？可以用否定词和转折词的组合效果来表达！
    5. 否定词不用多说。转折词的作用其实就是表达后句与前句的对立关系。
    6. 转折词和否定词均相当于一个“负号”。“组合效果”的道理如同：负负得正；双重否定表肯定。
    7. 因此，一句话里最终应该能提取出两个词列表。经过某种判断，一个可以归为pos列表，一个可以归为neg列表。
    """
    segmented_text_state, segmented_text_queue, candidates_queue, deny_list, transition_list, subjunctive_list, pos_seeds_list, neg_seeds_list, hownet_pos_list, hownet_neg_list = args_tuple
    while (not segmented_text_queue.empty()) or segmented_text_state.value=='have':
        #get()方法从队头删除并返回一个项目
        #如果队列为空，get()方法就使进程阻塞timeout秒。
        #如果在timeout秒内，发现可用的项目，则继续执行。如果超时，则引发一个异常。
        try:
            segmented_text_list = segmented_text_queue.get(block=True, timeout=0.1)
        except: continue 
        #deny_win = 3  #查找否定词的窗口大小，向前查找
        pos_candidates = dict()  #一个文本列表(100篇文章)拓展出的积极候选词
        neg_candidates = dict()  #一个文本列表(100篇文章)拓展出的消极候选词
        #-------------------------开始分析-------------------------------
        for temp_text in segmented_text_list:
            #因为是拓展中文词典，所以这里先过滤掉没有意义的字母、数字
            temp_text2 = re.sub('\w','',temp_text)
            #保留具有断句作用的标点，过滤掉其余的标点
            text = re.sub(':|：|\\.|\'|＇|‘|’|"|＂|“|”|、|\\\\|/','',temp_text2)
            #根据句末标点把文本分句
            sentence_list = re.split('。|!|！',text)
            for sentence in sentence_list:
                #针对出现连续标点的情况，如'a。!b' ---> ['a','','b']，标点之间有空格或者无空格
                if sentence.strip() == '': continue
                #一句话里应该能提取出两个词列表。最终可以经过某种判断对应为pos列表和neg列表。
                first_words_list = list()
                second_words_list = list()
                #一句话包括多个子句，划分子句
                sub_sentence_list = re.split(',|，|;|；|\\?|？', sentence)
                #转折词在子句之间起作用，否定词在子句之内起作用。
                #转折词一旦出现相当于一个“负号”，转折标记初始为1
                transition_sign = 1
                for sub_sentence in sub_sentence_list:
                    #针对出现连续标点的情况，如'a,;b' ---> ['a','','b']，标点之间有空格或者无空格
                    if sub_sentence.strip() == '': continue
                    temp_subsent_words = sub_sentence.split('|')
                    subsent_words = [word.strip() for word in temp_subsent_words if word.strip() != '']
                    if len(subsent_words)==0:
                        continue
                    #如果子句的首词是虚拟词，则不处理。
                    if subsent_words[0] in subjunctive_list:
                        continue
                    #从初始位置开始，处理该子句
                    initial = 0
                    #如果子句的首词是转折词，则transition_sign在上个子句的基础上取反
                    if subsent_words[0] in transition_list:
                        transition_sign = -transition_sign
                        if len(subsent_words)>1:
                            initial = 1
                        else:
                            continue
                    deny_sign = 1 #子句的否定标记默认为1，表示肯定
                    for word in subsent_words[initial:]:
                        if word in deny_list:
                            deny_sign = -deny_sign
                            continue
                        else:
                            sign = transition_sign*deny_sign
                            if sign==1:
                                first_words_list.append(word) #词列表里面的词允许重复，出现次数越多，重复越多。
                            else:
                                second_words_list.append(word)
                #整个句子处理完了，统计结果
                if len(first_words_list)<len(second_words_list):
                    temp_list = first_words_list
                    first_words_list = second_words_list
                    second_words_list = temp_list
                pos_count = 0
                neg_count = 0
                for word in first_words_list:
                    if word in pos_seeds_list:
                        pos_count += 1
                    if word in neg_seeds_list:
                        neg_count += 1
                if pos_count+neg_count == 0:
                    continue
                if pos_count>neg_count:
                    temp_pos_list = first_words_list
                    temp_neg_list = second_words_list
                else:
                    temp_neg_list = first_words_list
                    temp_pos_list = second_words_list
                #总会有错分的情况。这个时候我们使用知网词库，强制纠错。
                for pos in temp_pos_list:
                    if pos in hownet_neg_list:
                        del  temp_pos_list[temp_pos_list.index(pos)]
                        temp_neg_list.append(pos)
                for neg in temp_neg_list:
                    if neg in hownet_pos_list:
                        del temp_neg_list[temp_neg_list.index(neg)]
                        temp_pos_list.append(neg)
                #拓展出积极候选词
                for pos in temp_pos_list:       
                    if pos not in pos_candidates:   
                        pos_candidates[pos] = 1
                    else:
                        pos_candidates[pos] +=1
                #拓展出消极候选词
                for neg in temp_neg_list:       
                    if neg not in neg_candidates:   
                        neg_candidates[neg] = 1
                    else:
                        neg_candidates[neg] +=1
        #-------------------------获得候选词-----------------------------
        #一个文本列表(100篇文章)拓展出的候选词
        candidates_dict_tuple = (pos_candidates, neg_candidates)
        #put()方法在队尾插入一个项目
        #如果队列已满，put()方法就使子进程阻塞,直到空出一个数据单元
        candidates_queue.put(candidates_dict_tuple, block=True, timeout=None)
    return
    
#在候选词中挑选出真正可靠的pos词和neg词
def pick_authentic_dict(candidates_queue, dict_save_file):
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
        pos_candidates,neg_candidates = candidates_tuple
        #汇总拓展出的积极候选词
        for pos,counts in pos_candidates.items():
            if pos not in gather_pos_candidates:
                gather_pos_candidates[pos] = counts
            else:
                gather_pos_candidates[pos] += counts
        #汇总拓展出的消极候选词
        for neg,counts in neg_candidates.items():
            if neg not in gather_neg_candidates:
                gather_neg_candidates[neg] = counts
            else:
                gather_neg_candidates[neg] += counts
                
    print("-------------------------筛选候选词-----------------------------")
    #对于同属于两类的词，积极的词频大于消极的2倍才算积极的，否则就算作消极的。
    alpha = 1
    for word in gather_pos_candidates.keys():
        if word in gather_neg_candidates:
            if gather_pos_candidates[word] > gather_neg_candidates[word]*alpha :
                gather_neg_candidates[word] = 0
            else:
                gather_pos_candidates[word] = 0
    print("-------------------------筛选完毕------------------------------")
    print("-------------------------开始排序------------------------------")
    sorted_pos = sorted(gather_pos_candidates.items(), key=lambda d:d[1], reverse=True)
    sorted_neg = sorted(gather_neg_candidates.items(), key=lambda d:d[1], reverse=False) 
    print("-------------------------排序完毕------------------------------")
    print("-------------------------写入文件------------------------------")
    with open(dict_save_file,'w') as fw:
        if len(sorted_pos)>0 and sorted_pos[0][1]>0:
            pos_max_counts = sorted_pos[0][1]
            for word,counts in sorted_pos:
                if counts > 0 :
                    score = round(counts*1.0/pos_max_counts, 5)
                    fw.write(word + "\t" + str(score) + "\n")
        if len(sorted_neg)>0 and sorted_neg[-1][1]>0:
            neg_max_counts = sorted_neg[-1][1]
            for word,counts in sorted_neg:
                if counts > 0 :
                    score = round(-counts*1.0/neg_max_counts, 5)
                    fw.write(word + "\t" + str(score) + "\n") 
        fw.flush()
        fw.close()
    print("-------------------------写入完毕------------------------------")
    return
    
if __name__ == '__main__':
    start_time = time.time()
    
    #获得初始输入参数
    args = get_initial_args()    

    #创建必要的词列表
    deny_list = build_words_list(deny_words_file) #否定词
    transition_list = build_words_list(transition_words_file) #转折词
    subjunctive_list = build_words_list(subjunctive_words_file) #虚拟词
    #把种子词典导入相应的pos词列表和neg词列表
    pos_seeds_list, neg_seeds_list = seedwords_to_list(seeds_dict_file)
    #把知网词库导入到相应的pos词列表和neg词列表
    hownet_pos_list,hownet_neg_list = hownet_to_list(Hownet_poswords_file, Hownet_poswords_file)
    
    manager = multiprocessing.Manager()
    segmented_text_queue = manager.Queue(maxsize=Queue_SIZE) #创建分词文本队列
    #设置分词文本标记，用来表示分词的文本是否读入完毕。segmented_text_sign是一个对象。
    segmented_text_state = manager.Value(None,'have')
    candidates_queue = manager.Queue(maxsize=Queue_SIZE) #创建候选词队列    
    
    #创建进程池
    pool=multiprocessing.Pool(processes = args.processes)
    #创建一个读进程,将已经分词的文本读入分词文本队列。主进程不阻塞
    #python中函数传递参数：若参数是值类型则是值传递，如int、str；若参数是对象类型则是引用传递，如对象、list。
    #segmented_text_sign是一个对象，因此是引用传递。当引用的函数修改它时，主函数中它会相应改变，其他引用的函数中它也会相应改变。
    #因此可以用segmented_text_sign作为全局的标记。
    pool.apply_async(read_segmented_text, (segmented_corpus, segmented_text_queue , segmented_text_state))
    #pool.apply()为阻塞版本，主进程会被阻塞直到子进程执行结束

    #创建一个筛选候选词的进程，对所有的候选词进行统计分析，并将筛选结果写入文件。主进程不阻塞
    selector = multiprocessing.Process(target=pick_authentic_dict, args=(candidates_queue, expand_dict_file))
    selector.start() #启动写者进程
    
    #创建args.processes-1个拓展词典的进程，执行函数为expand_candidates_dict(args_tuple)。主进程不阻塞
    args_tuple = (segmented_text_state, segmented_text_queue, candidates_queue, deny_list, transition_list, subjunctive_list, pos_seeds_list, neg_seeds_list, hownet_pos_list, hownet_neg_list)
    for i in range(args.processes-1):
        #坑了很久! 在python中，括号()作用有二，表示(元组) 或 优先计算的(算术表达式)
        #如果括号里只有一个元素且不加逗号，python默认括号里是一个算术表达式
        #当加上一个逗号的时候，python解释器会明白这是一个tuple类型，而不是一个算术式。
        pool.apply_async( expand_candidates_dict, (args_tuple,) )
        #pool.apply()为阻塞版本，主进程会被阻塞直到子进程执行结束
    pool.close() #关闭进程池，不再接受新的进程
    pool.join() #等待读进程和词典拓展进程的结束
    
    #词典拓展进程全部结束后，向candidates_queue写入结束标记'over'，用来通知筛选者进程
    candidates_queue.put('over', block=True, timeout=None)
    selector.join() #等待筛选者进程结束
    
    end_time = time.time()
    delta_minutes = round((end_time-start_time)/60, 1)
    print("It takes %s minutes."%str(delta_minutes) )

    print("Done!")
    
    