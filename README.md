Sentiment Lexicon Induction
=============
目前的状况是
    1）有一套如何从ES发现行业对应文本的代码
    2）基于互信息的效果不好
    3）基于词向量的效果还不错

.
├── bin 里面有一个怎么根据xls更新情感词典txt的代码
├── domain_dict shukai.hu写的利用互信息的情感新词发现(xia.hong:效果并不好\
|   需要和word2vec结合起来，未实现； 作为一个互信息的baseline)
├── get_query_statement 获取领域对应的es查询语句
├── get_text    快速获取一个查询语句对应的文本
├── segment_words   多进程分词代码(shukai.hu)
└── word2vec_induction xia.hong实现的基于word2vec的情感新词发现
