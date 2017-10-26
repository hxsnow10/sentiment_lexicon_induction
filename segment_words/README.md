	脚本作用：
		使用哈工大分词模型对文本数据进行切词

	脚本用法：
		python seg_words_Ltp.py -p [进程数]
	    或者
		直接到脚本文件中更改参数，设置：源文本的位置、切词模型的位置

	文本格式：
		源文本:一行一篇文本，格式[来源\t文本标题\t文本内容]
		切词结果文本:一行一篇文本，只保存文本内容的切词结果，词之间用"|"分开

	哈工大切词模型位于:
		172.24.2.80/opt/shukai.hu/run_some/ltp_data_v3.4.0/cws.model
		以及
		FTP：lftp ftps://nlp_data:NcO6Nj4WiAI=@117.121.7.29 ltp_data_v3.4.0/cws.model