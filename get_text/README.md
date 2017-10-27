	------------------------get_text_using_existed_query.py---------------------
	背景：
		进行行业的情感分析，需要用到大量的针对某个行业的文本数据
		可以直接到ES中请求已经爬取到的行业文本数据
		针对某个行业的query语句已经保存在本地

	脚本作用：
		到ES中提取针对某个行业的文本，并保存到文件
		
	文本格式：
        一行一篇文本，格式[来源\t文本标题\t文本内容]

	脚本用法：
		python get_text_using_existed_query.py -p [进程数] -d [天数(days)，爬取最近days天的文本]
	    或者
		直接到脚本文件中更改参数，设置：爬取起始时间、爬取末尾时间、时间段切分大小、一次请求获得的文本数量、本地query语句位置、ES的ip地址等
	----------------------------------------------------------------------------------------------------------------------------------------------


	------------------------get_text_using_domain_words.py---------------------
	背景：
		获取行业文本......
		也可以利用一些行业词汇，直接到ES中请求包含这些行业词汇的文本
		行业词汇保存在domain_words.txt文件中

	脚本作用：
		到ES中提取针对某个行业的文本，并保存到文件
	
	文本格式：
        一行一篇文本，格式[来源\t文本标题\t文本内容]

	脚本用法：
		python get_text_using_domain_words.py -p [进程数] -d [天数(days)，爬取最近days天的文本]
	    或者
		直接到脚本文件中更改参数，设置：爬取起始时间、爬取末尾时间、时间段切分大小、一次请求获得的文本数量、本地query语句位置、ES的ip地址等

	行业词汇：
		行业词汇保存在domain_words.txt文件中，可以根据当前的需要进行删除或添加
	----------------------------------------------------------------------------------------------------------------------------------------------