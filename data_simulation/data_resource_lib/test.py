from data_simulation.utils.request_tool import request_and_get_result


result = request_and_get_result("https://hanyu.baidu.com/sentence/search?from=aladdin&gssda_res=%7B%22sentence_type%22%3A%22%E7%AD%BE%E5%90%8D%22%7D&query=%E4%B8%AA%E6%80%A7%E7%AD%BE%E5%90%8D%E5%A4%A7%E5%85%A8&smpid=&srcid=51451&tab_type=%E5%85%A8%E9%83%A8&wd=%E4%B8%AA%E6%80%A7%E7%AD%BE%E5%90%8D%E5%A4%A7%E5%85%A8&ret_type=mood")

print(result)
