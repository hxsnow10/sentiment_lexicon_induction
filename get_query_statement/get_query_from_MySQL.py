# -*- coding: utf-8 -*-
"""
Created on Thu Oct 12 20:28:13 2017

@author: hukai
"""
import pymysql
import argparse
import sys

#设置基本的参数
host_name = '172.24.5.130'
port_num = 3306
user_name = 'root'
pass_word = 'mf@168@mf'
object_database = 'bfd_mf_data_backup'
#可选的行业名：IT互联网、互联网金融、传统金融、政府、传媒、游戏、广告/公关/营销、咨询服务、电商/商超/贸易、消费品、
#           制造、房地产、建筑、家装、交通运输、汽车、旅游、酒店、食品餐饮、健康医疗、文体、教育、能源。
industry_name = "互联网金融"
query_statement_file = 'query_statement_for_domain.txt'

#获得初始输入参数
def get_initial_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--industry', action='store', default=industry_name, help="Set the industry name.Select from the following words:\n"+\
                        "IT互联网、互联网金融、传统金融、政府、传媒、游戏、广告/公关/营销、咨询服务、电商/商超/贸易、消费品、制造、"+\
                        "房地产、建筑、家装、交通运输、汽车、旅游、酒店、食品餐饮、健康医疗、文体、教育、能源")
    args = parser.parse_args()
    #输入的行业名应该包含在行业列表中
    industry_list = ['IT互联网','互联网金融','传统金融','政府','传媒','游戏','广告/公关/营销','咨询服务','电商/商超/贸易','消费品',
                     '制造','房地产','建筑','家装','交通运输','汽车','旅游','酒店','食品餐饮','健康医疗','文体','教育','能源']
    if args.industry not in industry_list:
        sys.exit("The industry name is wrong! Please select a industry from the following words:\n"+\
                 "IT互联网、互联网金融、传统金融、政府、传媒、游戏、广告/公关/营销、咨询服务、电商/商超/贸易、消费品、\n"+\
                 "制造、房地产、建筑、家装、交通运输、汽车、旅游、酒店、食品餐饮、健康医疗、文体、教育、能源")
    return args


if __name__ == '__main__':
    #获得初始输入参数
    args = get_initial_args()
    
    #打开数据库连接。存在中文的时候，连接需要添加charset='utf8'，否则中文显示乱码
    bfd_db = pymysql.connect(host=host_name, port=port_num, user=user_name, passwd=pass_word, db=object_database, charset='utf8')
    cursor = bfd_db.cursor() #创建游标
    
    industry_id_set = set() #获得行业id
    cursor.execute("select id from mf_industry where industry_name='%s'"%args.industry) #使用execute()方法执行SQL查询,并返回受影响的行数
    data_tuple = cursor.fetchall() #使用fetchall()方法获取所有结果数据
    #print(data_tuple)
    for item_tuple in data_tuple:
        industry_id, = item_tuple
        #print( isinstance(industry_id,int) )
        #print (industry_id)
        industry_id_set.add(industry_id)
        
    company_id_set = set() #获得目标行业的公司id
    for industry_id in industry_id_set:
        cursor.execute("select company_id from mf_company_industry_relation where industry_id=%d"%industry_id)
        data_tuple = cursor.fetchall() #使用fetchall()方法获取所有结果数据
        for item_tuple in data_tuple:
            company_id, = item_tuple
            #print( isinstance(company_id,int) )
            #print (company_id)
            company_id_set.add(company_id)
    
    topic_id_set = set() #获得和目标行业相关的所有topic_id
    for company_id in company_id_set:
        cursor.execute("select topic_ids from mf_role where company_id=%d"%company_id)
        data_tuple = cursor.fetchall() #使用fetchall()方法获取所有结果数据
        for topic_ids_tuple in data_tuple:
            topic_ids_str, = topic_ids_tuple
            topic_id_list = topic_ids_str.split(',')
            #set.update()方法把要传入的多元体拆分，每个元素作为个体加入集合中
            topic_id_set.update(topic_id_list)
            #获取的topic_id是unicode类型
    
    #mf_role表的topic_id即为mf_subject的lable_id
    #目标行业对应着若干公司，一个公司对应着若干个topic检索，一个topic检索对应着一个现成的query语句
    #将所有的query语句写到文件里，以供调用
    with open(query_statement_file,'w') as fw:
        for topic_id in topic_id_set:
            #获取的topic_id是unicode类型
            #print isinstance(topic_id,unicode)
            #print topic_id
            try:
                cursor.execute("select es_content from mf_subject where label_id=%d"%long(topic_id))
            except: continue
            data_tuple = cursor.fetchall() #使用fetchall()方法获取所有结果数据
            for es_content_tuple in data_tuple:
                es_content, =  es_content_tuple
                #获取的es_content是unicode类型
                #print isinstance(es_content,unicode)
                #print es_content
                fw.write(es_content.encode('utf-8')+'\n')
    
    bfd_db.close() #关闭数据库连接
    print("Done!")
    
