# 行情版客户端
import socket
import multiprocessing
import time
import struct
import os
import ctypes

# 配置地址端口用户口令
MARKET_CONF = {
	'HOST': '47.101.211.147',
	'PORT': 8003,
	'TOKEN': 'xxxxxx'
}

# 行情数据字段定义
class TDF_MARKET_DATA(ctypes.Structure): 
	_fields_ = [ 
		("自然日", ctypes.c_int),				# 业务发生日(自然日)
		("交易日", ctypes.c_int),				# 交易日
		("时间", ctypes.c_int),					# 时间(HHMMSSmmm)
		("状态", ctypes.c_int),					# 状态
		("前收盘价", ctypes.c_int64),			# 前收盘价
		("开盘价", ctypes.c_int64),				# 开盘价
		("最高价", ctypes.c_int64),				# 最高价
		("最低价", ctypes.c_int64),				# 最低价
		("最新价", ctypes.c_int64),				# 最新价
		("申卖价", ctypes.c_int64 * 10),			# 申卖价
		("申卖量", ctypes.c_int64 * 10),			# 申卖量
		("申买价", ctypes.c_int64 * 10),			# 申买价
		("申买量", ctypes.c_int64 * 10),			# 申买量
		("成交笔数", ctypes.c_int),				# 成交笔数
		("成交总量", ctypes.c_int64),			# 成交总量
		("成交总金额", ctypes.c_int64),			# 成交总金额
		("委托买入总量", ctypes.c_int64),	 		# 委托买入总量
		("委托卖出总量", ctypes.c_int64),	 		# 委托卖出总量
		("加权平均委买价格", ctypes.c_int64),		# 加权平均委买价格
		("加权平均委卖价格", ctypes.c_int64),		# 加权平均委卖价格
		("IOPV净值估值", ctypes.c_int),			# IOPV净值估值
		("到期收益率", ctypes.c_int),			# 到期收益率
		("涨停价", ctypes.c_int64),				# 涨停价
		("跌停价", ctypes.c_int64),				# 跌停价
		("证券信息前缀", ctypes.c_char * 4),		# 证券信息前缀
		("市盈率1", ctypes.c_int),				# 市盈率1
		("市盈率2", ctypes.c_int),				# 市盈率2
		("升跌", ctypes.c_int),					# 升跌（对比上一笔）
	] 

# 行情数据组合结构体
class TDF_MARKET(ctypes.Structure): 
	_fields_ = [ 
		("code", ctypes.c_int),
		("market", TDF_MARKET_DATA), 
	] 

# ====================================================================================================
# 获取行情数据
def get_market(my_conf):
	# 出错重连
	for x in range(3):
		try:
			# 定义参数
			my_host = my_conf['HOST']
			my_port = my_conf['PORT']
			my_token = my_conf['TOKEN']
			# 创建TCP套接字
			client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
			# 连接服务器
			client_socket.connect((my_host, my_port))
			# 设置连接参数
			client_socket.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 1024*1024)  # 发送缓存大小为1MB(默认64KB)
			client_socket.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 256*1024*1024)  # 接收缓存大小为256MB(默认64KB)
			client_socket.settimeout(15)
			# 发送口令登录
			client_socket.sendall(my_token.encode("UTF-8"))
			print(f"登录行情数据服务器[{my_host}：{my_port}@{my_token}]")
			# 登录成功，会收到数据(盘中)或心跳(休市)；
			# 登录失败，会收不到任何回应，并抛出超时。
			last_buffer = b'' # 存放未完整未处理的字节数据
			# 计算单笔数据大小
			format_size = 480 # 行情数据采用结构体，format_size = 480 bytes
			# 设置每次接收数据长度
			recv_len = int(format_size*100000)
			# =================================================================
			# 示例————输出到文件和屏幕打印笔数
			if not os.path.isfile("Sample_market.csv"):
				title = "本地时间, 证券代码, 行情时间, 开盘价, 最高价, 最低价, 最新价, 成交总量, 成交总金额, 十档申卖价, 十档申卖量, 十档申买价, 十档申买量\n"
			else:
				title = ""
			file_object = open("Sample_market.csv", "a")
			file_object.write(title)
			file_object.flush() 
			# =================================================================
			# 循环接收数据
			while True:
				# 定义数据列表
				datalist = [] 
				# 接收字节数据
				buffer = client_socket.recv(recv_len)
				# 新旧数据合并
				buffer = last_buffer + buffer 
				# 计算完整笔数
				data_count = int(len(buffer)/format_size)
				# 网络传输字节序列有时并非整笔到达，需要计算最后整笔位置
				end = data_count*format_size
				# 保存未完一笔
				last_buffer = buffer[end:] 
				# 处理完整逐笔
				offset = 0
				for i in range(data_count):
					# 取一笔进行处理，每次向后移动
					data = TDF_MARKET.from_buffer_copy(buffer, offset) # 行情数据采用结构体，format_size = 480 bytes
					offset += format_size
					stock_code = "%06d" % (data.code)
					# if stock_code=='000000': # 跳过心跳
					# 	continue
					datalist.append(data) # 加入数据列表以供后续使用
				# =================================================================
				# 用户定义如何使用数据
				# 示例————输出到文件和屏幕打印笔数
				now = time.strftime("%H:%M:%S", time.localtime(int(time.time())))
				for data in datalist: # 一行就是一笔数据，按字段顺序使用即可
					stock_code = "%06d" % (data.code)
					sellprice  = '|'.join(map(str, data.market.申卖价))
					sellvolume = '|'.join(map(str, data.market.申卖量))
					buyprice   = '|'.join(map(str, data.market.申买价))
					buyvolume  = '|'.join(map(str, data.market.申买量))
					file_object.write(now+", "+stock_code+", "+str(data.market.时间)+", "+str(data.market.开盘价)+", "+str(data.market.最高价)+", "+str(data.market.最低价)+", "+str(data.market.最新价)+", "+str(data.market.成交总量)+", "+str(data.market.成交总金额)+", "+sellprice+", "+sellvolume+", "+buyprice+", "+buyvolume+"\n")
				file_object.flush() 
				print(f"[{now}] 收到行情数据{len(datalist)}笔，写入文件Sample_market.csv")
				# =================================================================
				# time.sleep(1)
		except Exception as e:
			hourmin = int(time.strftime("%H%M", time.localtime(time.time())))
			if hourmin<910 and ("WinError" in str(e)): # 9点10分之前等待连接，之后连接不上报错
				print( f'行情数据服务等待连接...')
				time.sleep(15)
				continue
			# 关闭客户端
			client_socket.close()
			print(f"行情数据服务出错了({x+1})({e})")
			time.sleep(1)
			continue

# ====================================================================================================
if __name__ == '__main__':

	# 行情数据进程
	p_get_market = multiprocessing.Process(target=get_market, name='GET_MARKET', args=(MARKET_CONF, ), daemon=True)
	p_get_market.start()
	time.sleep(1)

	p_get_market.join()

	# 如有疑问+v：176516531

