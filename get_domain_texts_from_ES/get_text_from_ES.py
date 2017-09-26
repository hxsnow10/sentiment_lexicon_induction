# -*- coding: utf-8 -*-
"""
Created on Thu Sep 21 14:16:03 2017
@author: hukai
"""
#设置基本的参数
url = "http://172.24.4.21:9201"
save_file = "text_data.txt"
domain_words_file = "domain_words.txt"
days = 30
processes = 10

import argparse
import requests
import json
import re
import datetime
import multiprocessing
import sys

#获得初始输入参数
def get_initial_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('-p', dest='processes', type=int, default=processes, help="Set the process number. Must greater than 2.")
    parser.add_argument('-d', dest='days', type=int, default=days, help="Set the date range of text. Unit is day. Must greater than 1.")
    args = parser.parse_args()
    #若输入的参数不满足要求则退出程序
    #如果进程池的进程数小于2，程序就不能一边请求文本数据，一边写入磁盘
    if args.processes<2:
        sys.exit("The process number should be greater than 2.")
    if args.days<1:
        sys.exit("The date range of text should be greater than 1 day.")
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

def query(domain_words_list,time_slice_start,time_slice_end):
#==============================================================================
#   {"query":{ ... {
#                      "query_string": {
#                                       "default_field": "content",
#                                       "query": object_words_list [word1, word2, word3, ... ],
#                                       "default_operator":"AND"
#                                       }
#                   }
#              ...
#    }
#   这么一段历史代码是不可行的。
#   1.“query”后面跟一个匹配词列表object_words_list，让ES去查询含有这一系列词的文章，然后返回给我们，这符合需求。
#      但是不符合语法，“query”后面不能跟一个列表，只能跟一个词。
#   2.即使嵌套很多个 query_string 结构，每个结构里为 "query":object_word，这样也不行。
#      因为ES会对object_word继续分词，用分割后的小词去匹配文章。
#      “AND”只能保证所有的小词同时出现，却不能保证小词原来的顺序和邻接关系。
#      比如，有可能这样，“中国”会被分割成“中”和“国”。顺序和邻接关系被打乱了。而"match_phrase"正好满足这种需求!
#==============================================================================
    bool_should_list = list();
    for domain_word in domain_words_list:
        match_phrase_term ={
                            "match_phrase":{
                                            "content":domain_word
                                           }
                           }
        bool_should_list.append(match_phrase_term)
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
                                            "query":{
                                                     "bool":{
                                                             "should": bool_should_list
                                                            }
                                                     }
                                            
                                            }
                                  
                                  },
                        "from": 0,
                        "size": 1000,
                        "_source": [
                                    "docId",
                                    "docType",
                                    "title",
                                    "content"
                                   ]
                       }
    #print query_statement
    return query_statement
                                                                                
#进程请求的每篇文本保存为一个元组(text_from, text_title, text_content)
#一个进程的所有文本元组放进一个列表
#有多个进程进行网络请求，每个进程把自己的文本列表写入队列，写者进程从该队列批量获取文本
def get_text_data(arguments_tuple):
    #argument_tuple是输入参数构成的元组，包含如下：
    domain_words_list,text_queue,time_slice_start,time_slice_end = arguments_tuple
    search_url=url+"/bfd_mf/_search"
    query_statement = query(domain_words_list,time_slice_start,time_slice_end)
    response = requests.post(search_url, json.dumps(query_statement), timeout=None)
    #timeout属性设置超时时间，一旦超过这个时间还没获得响应内容，就会提示错误
    #返回的response是一个对象，利用response的属性可实现转化，转化为我们想要的类型
    #response.content是json格式的序列化结果，是字符串类型
    #print isinstance(response.content, str)
    #print isinstance(json.loads(response.content), dict)
    
    temp_hits_text_list = json.loads(response.content)["hits"]["hits"]
    #用json还原后的内容很多是unicode编码，需要将其重新编码为UTF-8格式
    hits_text_list = renew_encoding(temp_hits_text_list)
    
    save_text_list = list()
    for text_of_dict_type in hits_text_list:
        #一些文本的字典格式中没有’content‘键，直接去取会出错
        try:
            #提取文本来源
            text_from = text_of_dict_type['_source']['docType']
            
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
                #每篇文本保存为一个元组(text_from, text_title, text_content)
                #一个进程的所有文本元组放进一个列表
                save_text_list.append( (text_from,text_title,text_content) )
        except:
            continue
    #put()方法在队尾插入一个项目
    #如果队列已满，put()方法就使进程阻塞,直到空出一个数据单元
    text_queue.put(save_text_list, block=True, timeout=None)
    return

#让一个进程将文本写入磁盘
def write_to_file(text_queue, save_file):
    with open(save_file, 'w') as fw:
        while 1:
            #get()方法从队头删除并返回一个项目
            #如果队列为空，get()方法就使进程阻塞,直至有项目可用
            #每篇文本保存为一个元组(text_from, text_title, text_content),一个进程的所有文本元组放进一个列表
            text_list = text_queue.get(block=True, timeout=None)
            #将所有的文本数据写完之后，会取到一个结束标记'over'
            if text_list == 'over':
                break
            for text_tuple in text_list:
                #文本数据的本地保存方式，一行一篇文本，text_from - text_title - text_content
                fw.write(text_tuple[0] + "\t" + text_tuple[1] + "\t" + text_tuple[2] + "\n")
        fw.flush()
        fw.close()
    return

#把大的时间段切分为小的时间片
def segment_time(start_time,end_time):
    slice_start = start_time
    #因为str_time是按'年月日时分秒'的顺序排列的，所以可以直接用字符串比较大小，来表示时间先后
    while slice_start < end_time:
        slice_end = min( time_after_delta(slice_start, hours=6), end_time)
        yield (slice_start, slice_end)
        slice_start = slice_end    
    
if __name__=='__main__':
    #获得初始输入参数
    args = get_initial_args()
    
    #从文件读取检索词
    domain_words_list = read_words_from_file(domain_words_file)
    #print(search_words_list)
    
    #限定时间段，获取时间段之内的文本数据
    end_time = now()
    start_time = time_after_delta(end_time, days=-args.days)
    
    #创建多进程，因为网络请求消耗时间长，大部分时间处于等待状态
    #为了提高效率,我们让多个进程进行网络请求，让一个进程将文本写入磁盘
    manager = multiprocessing.Manager()
    text_queue = manager.Queue()
    pool=multiprocessing.Pool(processes = 10)
    #创建一个写者进程,主进程不阻塞
    pool.apply_async(write_to_file, (text_queue, save_file) )
    #pool.apply()为阻塞版本，主进程会被阻塞直到子进程执行结束
    
    #创建多个进程进行网络请求,执行函数为 get_text_data(arguments_tuple)
    #把大的时间段切分为小的时间片，一个网络请求进程负责获取一个时间片内的的文本数据
    #构建参数列表,列表的元素是arguments_tuple
    input_args_list = list()
    for slice_tuple in segment_time(start_time,end_time):
        slice_start, slice_end = slice_tuple
        input_args_list.append( (domain_words_list, text_queue, slice_start, slice_end) )
    pool.map(get_text_data, input_args_list )
    #多个进程进行网络请求时，主进程阻塞
    #等待所有的网络请求进程执行完毕，然后主进程恢复执行。并不等待写者进程。
    #pool.map_async()为非阻塞版本，主进程不阻塞
    
    #网络请求结束，向text_queue写入结束标记'over'，用来通知写者进程
    text_queue.put('over', block=True, timeout=None)
    pool.close() #关闭进程池，不再接受新的进程
    pool.join() #等待写者进程结束
    
    print("Done!")
