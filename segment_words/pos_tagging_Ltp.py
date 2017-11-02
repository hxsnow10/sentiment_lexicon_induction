# -*- coding: utf-8 -*-
"""
Created on Fri Sep 29 12:16:27 2017

@author: hukai
"""

segment_file = "seg_result_text1.txt"
tag_result_file = "pos_tagging_text.txt"
model_path = "E:/ltp_data_v3.4.0/pos.model" #Ltp3.4分词模型cws.model的路径
processes = 10 #分词子进程的数量
Queue_SIZE = 100 #共享队列能够容纳的元素个数

import argparse
import sys
import multiprocessing
import pyltp
import time

#==============================================================================
# 之所以不把这一段代码删掉，是因为这是一个大坑!
# 不知道具体为什么，只要加了这一段代码，print()就不能打印到控制台！
# 也可能是个例！
# reload(sys)
# sys.setdefaultencoding('utf-8') #设置UTF-8输出环境
#==============================================================================

def get_initial_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('-p', dest='processes', type=int, default=processes, help="Set the process number. Must greater than 0.")
    args = parser.parse_args()
    #若输入的参数不满足要求则退出程序
    #如果进程数小于3，程序就不能一边读入源文本，一边对文本分词，一边将分词结果写入文件
    if args.processes<1:
        sys.exit("The process number should be greater than 0.")
    return args

#创建一个读者进程,将源文本读入初始文本队列
#==============================================================================
#           文件读写的问题：
#             for line in fr.readlines()
#             for line in fr.readline()
#             for line in fr
#           三者都是可以的，但各有不同：
#               fr.read() 读取整个文本，用于将整个的文本内容放到一个字符串变量中。也可以用参数限定读取长度。
#               fr.readlines() 一次性读取整个文本，读取结果是一个行列表。当文件太大 或 没有足够内存 时，不可行。
#               fr.readline() 读取一行文本，读取结果是一个字符串。
#                   所以，for line in fr.readline()其实是遍历字符串的每一个字符，即for char in string
#                   如果想用fr.readline()按行读取整个文本，不应把它放到for...in的后面，而应该把它放到循环体内部。
#               python2.5之后已经把文件设置为一种可迭代类型，fr本身是可迭代的。
#                   for line in fr就相当于不断地调用fr.next()方法，对内存占用很少。
#                   所以当文件很大时，这是一种节约内存的读取方法！！！
#==============================================================================
def read_text_to_queue(text_file, text_queue, text_state):
    with open(text_file, 'r') as fr:
        while True:
            text_list = list()
            count = 0
            for segmented_text in fr:
                #文本数据的本地保存方式，一行一篇已经分词的文本
                text_list.append(segmented_text.strip())
                count += 1
                if count >= 100: break
            if count==0:break
            #put()方法在队尾插入一个项目
            #如果队列已满，put()方法就使子进程阻塞,直到空出一个数据单元
            text_queue.put(text_list, block=True, timeout=None)
        fr.close()
    text_state.set("empty")
    return

def tag_text(segment_text_state, segment_text_queue, tag_result_queue):
    postagger = pyltp.Postagger() #实例化分词模块
    postagger.load(model_path) #加载分词库
    while (not segment_text_queue.empty()) or segment_text_state.value=='have':
        #get()方法从队头删除并返回一个项目
        #如果队列为空，get()方法就使进程阻塞timeout秒。
        #如果在timeout秒内，发现可用的项目，则继续执行。如果超时，则引发一个异常。
        try:
            segment_text_list = segment_text_queue.get(block=True, timeout=0.1)   
            tag_text_list = list()
            #一个分词进程分的所有文本放进一个列表,一个列表项就是一篇文本
            for text in segment_text_list:
                words_list = text.split("|")
                postags_list = postagger.postag(words_list)
                tag_result_list = list()
                for word,postag in zip(words_list,postags_list):
                    tag_result_list.append(word+"/"+postag)
                tag_result_text = ' '.join(tag_result_list)
                tag_text_list.append(tag_result_text)
            #put()方法在队尾插入一个项目
            #如果队列已满，put()方法就使子进程阻塞,直到空出一个数据单元
            tag_result_queue.put(tag_text_list, block=True, timeout=None)
        except:pass
    return

#创建一个写者进程将标注结果写入磁盘
def write_text_to_file(text_queue, text_file):
    with open(text_file, 'w') as fw:
        while True:
            #get()方法从队头删除并返回一个项目
            #如果队列为空，get()方法就使进程阻塞,直至有项目可用
            #一个标注进程分的所有文本放进一个列表,一个列表项就是一篇文本
            text_list = text_queue.get(block=True, timeout=None)
            #将所有的文本数据写完之后，会取到一个结束标记'over'
            if text_list == 'over':
                break
            for text in text_list:
                #分词的文本在本地保存为，一行一篇
                fw.write(text + "\n")
        fw.flush()
        fw.close()
    return

if __name__ == '__main__':
    '''
    为了提高词性标注效率，利用多进程进行标注:
    一个读者进程，负责从分词文件中读取数据到分词文本队列
    多个标注进程，从初始文本队列获取文本，然后标注，将标注结果写到标注结果队列
    一个写者进程，负责将标注结果队列中的文本写入目标文件
    '''
    #--------------------------调试--------------------------------
    #print("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")
    #sys.stdout.flush()
    start_time = time.time()
    
    #获得初始输入参数
    args = get_initial_args()

    manager = multiprocessing.Manager()
    segment_text_queue = manager.Queue(maxsize=Queue_SIZE) #创建分词文本队列
    #设置分词文本标记，用来表示分词的文本是否读入完毕。segment_text_state是一个对象。
    segment_text_state = manager.Value(None,'have')   
    tag_result_queue = manager.Queue(maxsize=Queue_SIZE) #创建标注结果队列
    
    #创建一个读者进程,将分词文本读入分词文本队列。主进程不阻塞
    #python中函数传递参数：若参数是值类型则是值传递，如int、str；若参数是对象类型则是引用传递，如对象、list。
    #segment_text_state是一个对象，因此是引用传递。当引用的函数修改它时，主函数中它会相应改变，其他引用的函数中它也会相应改变。
    #因此可以用segment_text_state作为全局的标记。
    #在这里犯了一个大错误，参数未对齐! Process类的第一个参数是group，不是target，如果不指明的话就会出错!
    #multiprocessing.Process类的定义:__init__(self, group=None, target=None, name=None, args=(), kwargs={})
    reader = multiprocessing.Process(target=read_text_to_queue, args=(segment_file, segment_text_queue, segment_text_state))
    reader.start() #启动读者进程

    #创建一个写者进程,将标注文本从标注结果队列写入文件。主进程不阻塞
    writer = multiprocessing.Process(target=write_text_to_file, args=(tag_result_queue, tag_result_file))
    writer.start() #启动写者进程
    
    #创建进程池。一个进程池维护着一个任务队列_taskqueue。池中的processes个worker进程到任务队列中获取任务。
    pool=multiprocessing.Pool(processes=args.processes)
    
    #创建args.processes个标注进程，执行函数为 tag_text(segment_text_state, segment_text_queue, tag_result_queue)。主进程不阻塞
    for i in range(args.processes):
        pool.apply_async( tag_text, (segment_text_state, segment_text_queue, tag_result_queue) )
        #每调用一次apply_async方法，实际上是向_taskqueue中添加了一条任务
        #apply_async方法中新建的任务只是被添加到任务队列中，并不一定有worker进程立马去执行
        #pool.apply()为阻塞版本，主进程会被阻塞直到子进程执行结束
    pool.close() #关闭进程池，不再接受新的进程
    reader.join() #等待读者进程结束
    pool.join() #等待分词进程结束
    
    #分词进程全部结束后，向seg_result_queue写入结束标记'over'，用来通知写者进程
    tag_result_queue.put('over', block=True, timeout=None)
    writer.join() #等待写者进程结束
    
    end_time = time.time()
    delta_minutes = round((end_time-start_time)/60, 1)
    print("It takes %s minutes."%str(delta_minutes) )

    print("Done!")
    