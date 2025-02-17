# 全推版客户端
import socket
import multiprocessing
import time
import struct
import os

# 配置地址端口用户口令
TRANS_CONF = {
	'HOST': 'x.x.x.x',
	'PORT': 9001,
	'TOKEN': 'test'
}

ORDER_CONF = {
	'HOST': 'x.x.x.x',
	'PORT': 9002,
	'TOKEN': 'test'
}

# ====================================================================================================
# 获取成交数据
def get_trans(my_conf):
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
			print(f"登录成交数据服务器[{my_host}：{my_port}@{my_token}]")
			# 登录成功，会收到数据(盘中)或心跳(休市)；
			# 登录失败，会收不到任何回应，并抛出超时。
			last_buffer = b'' # 存放未完整未处理的字节数据
			# 计算单笔数据大小
			format_size = struct.calcsize(">6s4IQ2B2I2s") # 网络传输数据采用大端模式，format_size = 42 bytes
			# 生成解包对象示例
			mystruct = struct.Struct(">6s4IQ2B2I2s")
			# 设置每次接收数据长度
			recv_len = int(format_size*100000)
			# =================================================================
			# 示例————输出到文件和屏幕打印笔数
			if not os.path.isfile("Sample_Trans.csv"):
				title = "本地时间, 证券代码, 成交时间, 成交编号, 成交单价, 成交数量, 成交金额, 交易方向, 交易类型, 买方委托编号, 卖方委托编号, 结束符号\n"
			else:
				title = ""
			file_object = open("Sample_Trans.csv", "a")
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
					data = mystruct.unpack_from(buffer, offset) # 网络传输数据采用大端模式，format_size = 42 bytes
					offset += format_size
					if data[0]==b'HEARTB': # 跳过心跳
						continue
					datalist.append(data) # 加入数据列表以供后续使用
				# print(data)
				# =================================================================
				# 用户定义如何使用数据
				# 示例————输出到文件和屏幕打印笔数
				now = time.strftime("%H:%M:%S", time.localtime(int(time.time())))
				for line in datalist:
					line = list(line)
					line[0] = line[0].decode()
					line[10] = line[10].decode()
					file_object.write(now+", "+str(line)+"\n")
				file_object.flush() 
				print(f"[{now}] 收到成交数据{len(datalist)}笔，写入文件Sample_Trans.csv")
				# =================================================================
				# time.sleep(1)
		except Exception as e:
			hourmin = int(time.strftime("%H%M", time.localtime(time.time())))
			if hourmin<910 and ("WinError" in str(e)): # 9点10分之前等待连接，之后连接不上报错
				print(f'成交数据服务等待连接...')
				time.sleep(15)
				continue
			# 关闭客户端
			client_socket.close()
			print(f"成交数据服务出错了({x+1})({e})")
			time.sleep(1)
			continue

# 获取委托数据
def get_order(my_conf):
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
			print(f"登录委托数据服务器[{my_host}：{my_port}@{my_token}]")
			# 登录成功，会收到数据(盘中)或心跳(休市)；
			# 登录失败，会收不到任何回应，并抛出超时。
			last_buffer = b'' # 存放未完整未处理的字节数据
			# 计算单笔数据大小
			format_size = struct.calcsize(">6s4I2B2s") # 网络传输数据采用大端模式，format_size = 26 bytes
			# 生成解包对象示例
			mystruct = struct.Struct(">6s4I2B2s")
			# 设置每次接收数据长度
			recv_len = int(format_size*100000)
			# =================================================================
			# 示例————输出到文件和屏幕打印笔数
			if not os.path.isfile("Sample_Order.csv"):
				title = "本地时间, 证券代码, 委托时间, 委托编号, 委托单价, 委托数量, 交易方向, 交易类型, 结束符号\n"
			else:
				title = ""
			file_object = open("Sample_Order.csv", "a")
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
					data = mystruct.unpack_from(buffer, offset) # 网络传输数据采用大端模式，format_size = 42 bytes
					offset += format_size
					if data[0]==b'HEARTB': # 跳过心跳
						continue
					datalist.append(data) # 加入数据列表以供后续使用
				# print(data)
				# =================================================================
				# 用户定义如何使用数据
				# 示例————输出到文件和屏幕打印笔数
				now = time.strftime("%H:%M:%S", time.localtime(int(time.time())))
				for line in datalist:
					line = list(line)
					line[0] = line[0].decode()
					line[7] = line[7].decode()
					file_object.write(now+", "+str(line)+"\n")
				file_object.flush() 
				print(f"[{now}] 收到委托数据{len(datalist)}笔，写入文件Sample_Order.csv")
				# =================================================================
				# time.sleep(1)
		except Exception as e:
			hourmin = int(time.strftime("%H%M", time.localtime(time.time())))
			if hourmin<910 and ("WinError" in str(e)): # 9点10分之前等待连接，之后连接不上报错
				print( f'委托数据服务等待连接...')
				time.sleep(15)
				continue
			# 关闭客户端
			client_socket.close()
			print(f"委托数据服务出错了({x+1})({e})")
			time.sleep(1)
			continue

# ====================================================================================================
if __name__ == '__main__':

	
	# 成交数据进程
	p_get_trans = multiprocessing.Process(target=get_trans, name='GET_TRANS', args=(TRANS_CONF, ), daemon=True)
	p_get_trans.start()
	time.sleep(1)

	# 委托数据进程
	p_get_order = multiprocessing.Process(target=get_order, name='GET_ORDER', args=(ORDER_CONF, ), daemon=True)
	p_get_order.start()
	time.sleep(1)

	p_get_trans.join()
	p_get_order.join()


