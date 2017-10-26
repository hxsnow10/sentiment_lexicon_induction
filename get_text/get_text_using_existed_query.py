# -*- coding: utf-8 -*-
"""
Created on Thu Sep 21 14:16:03 2017

@author: hukai
"""
#设置基本的参数
url = "http://172.24.4.21:9201"
save_file = "text_data.txt"
bool_query_file = "../get_query_statement/query_statement_for_domain.txt"
processes = 30 #子进程的数量
days = 90 #从Es中获取最近days天的文本
time_slice_by_hour = 24 #时间片大小(小时)，一个进程从ES中获取一个时间片范围内的文本
texts_amount = 10000 #在一个时间片范围内获取的文本数
Queue_SIZE = 100 #共享队列能够容纳的元素个数

import argparse
import requests
import json
import re
import datetime
import multiprocessing
import sys

#python专门设置了一种机制用来防止无限递归，防止内存溢出崩溃
#对于字典、列表、元组的嵌套来说，有一个最大嵌套层数，不能无穷嵌套
#默认的最大递归数是976
#sys.setrecursionlimit(10000)

#获得初始输入参数
def get_initial_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('-p', dest='processes', type=int, default=processes, help="Set the process number. Must greater than 1.")
    parser.add_argument('-d', dest='days', type=int, default=days, help="Set the date range of text. Unit is day. Must greater than 0.")
    args = parser.parse_args()
    #若输入的参数不满足要求则退出程序
    #如果进程池的进程数小于2，程序就不能一边请求文本数据，一边写入磁盘
    if args.processes<2:
        sys.exit("The process number should be greater than 1.")
    if args.days<1:
        sys.exit("The date range of text should be greater than 0 day.")
    return args

#从文件读取检索词
def read_words_from_file(domain_words_file):
    domain_words_list = list()
    with open(domain_words_file, 'r') as fr:
        for word in fr.readlines():
            domain_words_list.append( word.strip() )
        fr.flush()
        fr.close()
    return domain_words_list

#datetime是模块，datetime模块还包含一个datetime类
#datetime.datetime.now()返回当前日期和时间，其类型是datetime，时间元组格式为 (年,月,日,时,分,秒,~)
#isinstance(datetime.now(),datetime)
def now():
    return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
def str2time(str_time):
    return datetime.datetime.strptime(str_time, "%Y-%m-%d %H:%M:%S")
def time2str(time_tuple):
    return time_tuple.strftime("%Y-%m-%d %H:%M:%S")
def time_after_delta(str_time,days=0, hours=0, minutes=0):
    return time2str(str2time(str_time)+datetime.timedelta(days=days,hours=hours,minutes=minutes))

#用json还原后的内容很多是unicode编码，需要将其重新编码为UTF-8格式
def renew_encoding(inputs):
    if isinstance(inputs, dict):
        return {renew_encoding(key): renew_encoding(value)
                for key, value in inputs.iteritems()}
    elif isinstance(inputs, list):
        return [renew_encoding(element) for element in inputs]
    elif isinstance(inputs, unicode):
        return inputs.encode('utf-8')
    else:
        return inputs

def get_direct_query(query_file):
    query_str_list = list()
    with open(query_file,'r') as fr:
        k = 0
        for line in fr.readlines():
            k += 1
            query_statement_str = line.strip().split('=')[1]
            #print query_statement_str
            #把字符串格式转化为字典格式
            #print isinstance(query_statement_str,str)
            #query_statement = json.loads(query_statement_str)
            #print isinstance(query_statement,dict)
            #print query_statement
            #query_list.append(query_statement)
            
            #此处不把字符串格式转化为字典格式，而是将这个转化过程推迟到请求进程中去做
            #传递的是字符串，而不是多层嵌套的字典，multiprocessing.pool.map()处理起来会更快
            query_str_list.append(query_statement_str)
            #if k==10: break
    return query_str_list

def query(bool_query,time_slice_start,time_slice_end):
    query_statement = {"query":{
                                "filtered":{
                                            "filter":{
                                                      "bool":{
                                                              "must":[
                                                                      {
                                                                       "range":{
                                                                                "createTimeStr":{
                                                                                                 "gte": time_slice_start,
                                                                                                 "lt": time_slice_end
                                                                                                 }
                                                                                }
                                                                       },                                
                                                                       {
                                                                        "exists":{
                                                                                 "field":"content"
                                                                                 }
                                                                        }
                                                                     ],
                                                              "must_not":[],
                                                              "should":[]
                                                              }
                                                      },
                                            "query":bool_query
                                            }
                                  
                                  },
                        "from": 0,
                        "size": texts_amount,
                        "_source": [
                                    "docId",
                                    "docType",
                                    "title",
                                    "content"
                                   ]
                       }
    #print query_statement
    return query_statement

#进程请求的每篇文本保存为一个元组(docId, text_from, text_title, text_content)
#一个进程的所有文本元组放进一个列表
#有多个进程进行网络请求，每个进程把自己的文本列表写入队列，写者进程从该队列批量获取文本
def get_text_data(arguments_tuple):
    #argument_tuple是输入参数构成的元组，包含如下：
    query_str_list,text_queue,time_slice_start,time_slice_end = arguments_tuple
    #print bool_query_list
    search_url=url+"/bfd_mf/_search"
    total_request = len(query_str_list)
    count = 0
    for bool_query_str in query_str_list:
        bool_query = json.loads(bool_query_str)
        query_statement = query(bool_query, time_slice_start, time_slice_end)
        response = requests.post(search_url, json.dumps(query_statement), timeout=None)
        #timeout属性设置超时时间，一旦超过这个时间还没获得响应内容，就会提示错误
        #返回的response是一个对象，利用response的属性可实现转化，转化为我们想要的类型
        #response.content是json格式的序列化结果，是字符串类型
        #print isinstance(response.content, str)
        #print isinstance(json.loads(response.content), dict)
        percent = round(count*100.0/total_request,1)
        print str(response)+"\t"+str(percent)+"%"
        count += 1
        #print response.content
        #ftemp = open('temp.txt','w')
        #ftemp.write(response.content)
        #ftemp.close()
        
        standard_content_strc = json.loads(response.content)
        if "hits" not in standard_content_strc: return
        if "hits" not in standard_content_strc["hits"]: return
        #用json还原后的内容是unicode编码，需要将其重新编码为UTF-8格式
        hits_text_list = renew_encoding( standard_content_strc["hits"]["hits"] )
        
        save_text_list = list()
        for text_of_dict_type in hits_text_list:
            #一些文本的字典格式中没有’content‘键，直接去取会出错
            try:
                #提取文本来源
                text_from = text_of_dict_type['_source']['docType']
                #提取文本id
                docId = text_of_dict_type['_source']['docId']
                
                #提取文本标题
                temp_text_title = text_of_dict_type['_source']['title']
                #去掉文本标题中的所有空字符“\n”、“\t”、“ ”、“　”，尤其要注意全角格式的空格字符“　”
                temp_title_list = re.split('\n|\t| |　',temp_text_title)
                text_title = ''.join(temp_title_list)
                
                #提取文本内容
                temp_text_content = text_of_dict_type['_source']['content']
                #去掉文本内容中的所有空字符“\n”、“\t”、“ ”、“　”，尤其要注意全角格式的空格字符“　”
                temp_content_list = re.split('\n|\t| |　',temp_text_content)
                text_content = ''.join(temp_content_list)
                
                #太短的文本不要
                if len(text_content)>50:
                    #每篇文本保存为一个元组(docId, text_from, text_title, text_content)
                    #一个进程的所有文本元组放进一个列表
                    save_text_list.append( (docId,text_from,text_title,text_content) )
            except:
                continue
        #print text_queue.qsize()
        #put()方法在队尾插入一个项目
        #如果队列已满，put()方法就使进程阻塞,直到空出一个数据单元
        text_queue.put(save_text_list, block=True, timeout=None)
    return

#让一个进程将文本写入磁盘
def write_to_file(text_queue, save_file):
    with open(save_file, 'w') as fw:
        title_set = set()
        while True:
            #get()方法从队头删除并返回一个项目
            #如果队列为空，get()方法就使进程阻塞,直至有项目可用
            #每篇文本保存为一个元组(docId, text_from, text_title, text_content),一个进程的所有文本元组放进一个列表
            text_list = text_queue.get(block=True, timeout=None)
            #将所有的文本数据写完之后，会取到一个结束标记'over'
            if text_list == 'over':
                break
            for text_tuple in text_list:
                #利用text_title（即text_tuple[2]）对重复的文本过滤
                if text_tuple[2] not in title_set:
                    title_set.add( text_tuple[2] )
                    #文本数据的本地保存方式，一行一篇文本，text_from - text_title - text_content
                    fw.write(text_tuple[1] + "\t" + text_tuple[2] + "\t" + text_tuple[3] + "\n")
        fw.flush()
        fw.close()
    return

#把大的时间段切分为小的时间片
def segment_time(start_time,end_time):
    slice_start = start_time
    #因为str_time是按'年月日时分秒'的顺序排列的，所以可以直接用字符串比较大小，来表示时间先后
    while slice_start < end_time:
        slice_end = min( time_after_delta(slice_start, hours=time_slice_by_hour), end_time)
        yield (slice_start, slice_end)
        slice_start = slice_end    
    
if __name__=='__main__':
    #获得初始输入参数
    args = get_initial_args()
    
    #从文件中读取query语句
    query_str_list = get_direct_query(bool_query_file)
    #print(bool_query_str_list)
    
    #限定时间段，获取时间段之内的文本数据
    end_time = now()
    start_time = time_after_delta(end_time, days=-args.days)
    
    #创建多进程，因为网络请求消耗时间长，大部分时间处于等待状态
    #为了提高效率,我们让多个进程进行网络请求，让一个进程将文本写入磁盘
    manager = multiprocessing.Manager()
    text_queue = manager.Queue(maxsize=Queue_SIZE)
    pool=multiprocessing.Pool(processes = args.processes)
    #创建一个写者进程,主进程不阻塞
    pool.apply_async(write_to_file, (text_queue, save_file) )
    #pool.apply()为阻塞版本，主进程会被阻塞直到子进程执行结束
    
    #创建多个进程进行网络请求,执行函数为 get_text_data(arguments_tuple)
    #把大的时间段切分为小的时间片，一个网络请求进程负责获取一个时间片内的的文本数据
    #构建参数列表,列表的元素是arguments_tuple
    input_args_list = list()
    for slice_tuple in segment_time(start_time,end_time):
        slice_start, slice_end = slice_tuple
        input_args_list.append( (query_str_list, text_queue, slice_start, slice_end) )
    pool.map(get_text_data, input_args_list )
    #多个进程进行网络请求时，主进程阻塞
    #等待所有的网络请求进程执行完毕，然后主进程恢复执行。并不等待写者进程。
    #pool.map_async()为非阻塞版本，主进程不阻塞
    
    #网络请求结束，向text_queue写入结束标记'over'，用来通知写者进程
    text_queue.put('over', block=True, timeout=None)
    pool.close() #关闭进程池，不再接受新的进程
    pool.join() #等待写者进程结束
    
    print("Done!")
