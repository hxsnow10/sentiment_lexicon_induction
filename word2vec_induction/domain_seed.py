# encoding=utf-8
def read_vocab_count(vocab_path):
    word_count={}
    if vocab_path: 
        ii=open(vocab_path)
        for line in ii:
            try:
                w,c=line.strip().split()
                word_count[w]=int(c)
            except Exception:
                print line
    return word_count 

def ratio(word_count):
    s=sum(word_count.values())
    for w in word_count:
        word_count[w]=1.0*word_count[w]/s
    return word_count

def domain_seed(senti_dict_path, vocab_paths):
    
    ii=open(senti_dict_path,'r')
    senti_dict={}
    for i in ii:
        w,p=i.strip().split()
        if p not in ['0','1','-1']:continue
        senti_dict[w]=p
    ii.close()
    
    word_counts = [read_vocab_count(vocab_path) for vocab_path in vocab_paths]
    word_scores = [ratio(word_count) for word_count in word_counts]
    
    oo=open('dmain_seeds.txt','w')
    oo.write('word\t{}\n'.format('\t'.join(vocab_paths)))
    for w in word_scores[0]:
        if w not in senti_dict:continue
        scores=[word_score[w] for word_score in word_scores if w in word_score]
        if len(scores)!=len(word_scores):continue
        mean=1.0*sum(scores)/len(scores)
        #print scores, mean
        scores=[(score-mean)/mean for score in scores]
        oo.write(w+'\t{}\t{}\t{}\n'.format('\t'.join([str(x) for x in scores]), mean, senti_dict[w]))

vocab_dir='/opt/xia.hong/get_es_text/'
vocab_paths=[vocab_dir+'vocab_jingrong.txt', vocab_dir+'vocab_canguan.txt', vocab_dir+'vocab_yiliao.txt']
domain_seed('senti_dict.txt',vocab_paths)
