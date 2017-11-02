# -*- coding: utf-8 -*-
"""
Created on Wed Nov 01 11:27:32 2017

@author: hukai
"""

seg_file = "seg_食品餐饮.txt"
seed_file = "../domain_dict/senti_dict.txt"
save_file = "count_existed_seed_食品餐饮.txt"

if __name__=='__main__':
    fr = open(seed_file,'r')
    seed_dict = dict()
    for line in fr.readlines():
        word_senti = line.split()
        if word_senti[0] not in seed_dict:
            seed_dict[word_senti[0]] = word_senti[1]
    fr.close()
    
    fr = open(seg_file,'r')
    count_seed_in_text = dict()
    for line in fr:
        word_list = line.split()
        for word in word_list:
            if word not in count_seed_in_text:
                count_seed_in_text[word] = 1
            else:
                count_seed_in_text[word] += 1
    fr.close()
    
    seed_num_dict = dict()
    for seed in seed_dict:
        if seed in count_seed_in_text:
            seed_num_dict[seed] = count_seed_in_text[seed]
    
    sorted_seed = sorted(seed_num_dict.items(), key=lambda d:d[1], reverse=True)
    
    fw = open(save_file,'w')
    for seed,num in sorted_seed:
        fw.write(seed+"\t"+seed_dict[seed]+"\t"+str(num)+"\n")
    fw.flush()
    fw.close()
        
    print("Done!")
        
        
    
        
        