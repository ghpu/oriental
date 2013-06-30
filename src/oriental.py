#!/usr/bin/env python
# _*_ coding:utf_8 _*_

# Binary Protocol tentative for OrientDB 1.4

import os
import sys
import struct
import socket
from collections import OrderedDict
import time


class Status:
	"""
	Contains an enum for all return status codes
	"""
	values = ['OK','DISCONNECTED','CONNECTIONFAILED']

	class __metaclass__(type):
		def __getattr__(self, name):
			return (self.values.index(name),name)
		def __setattr__(self,name,value):
			raise NotImplementedError

	def name_of(self, i):
		return self.values[i]
	
	def tuples(self):
		return tuple(enumerate(Status.values))

	def __repr__(self):
		return str(self.tuples())

class Connection:
	"""
	Represent a connection to a OrientDB server
	"""

	def __init__(self,host="localhost",port=2424,protocol="http",user="ghpu",password="ghpu"):
		self.protocol=protocol
		self.host=host
		self.port=port
		self.user=user
		self.password=password

	def get_url(self):
		return (self.host,self.port)

	def __repr__(self):
		return str(self.__class__)+" "+self.getUrl()

class Request:
	SHUTDOWN =1
	CONNECT =2
	DB_OPEN =3
	DB_CREATE =4
	DB_CLOSE =5
	DB_EXIST =6
	DB_DROP =7
	DB_SIZE =8
	DB_COUNTRECORDS =9
	DATACLUSTER_ADD =10
	DATACLUSTER_DROP =11
	DATACLUSTER_COUNT =12
	DATACLUSTER_DATARANGE =13
	DATACLUSTER_COPY =14
	DATACLUSTER_LH_CLUSTER_IS_USED =16
	DATASEGMENT_ADD =20
	DATASEGMENT_DROP =21
	REQUEST_RECORD_METADATA =29
	RECORD_LOAD =30
	RECORD_CREATE =31
	RECORD_UPDATE =32
	RECORD_DELETE =33
	RECORD_COPY =34
	REQUEST_RECORD_CHANGE_IDENTITY =35
	REQUEST_POSITIONS_HIGHER =36
	REQUEST_POSITIONS_LOWER =37
	REQUEST_RECORD_CLEAN_OUT =38
	REQUEST_POSITIONS_FLOOR =39
	COUNT =40
	COMMAND =41
	REQUEST_POSITIONS_CEILING =42
	TX_COMMIT =60
	CONFIG_GET =70
	CONFIG_SET =71
	CONFIG_LIST =72
	DB_RELOAD =73
	DB_LIST =74
	REQUEST_PUSH_RECORD =74
	REQUEST_PUSH_DISTRIB_CONFIG =74
	REQUEST_DB_COPY =74
	REQUEST_REPLICATION =74
	REQUEST_CLUSTER =74
	REQUEST_DB_TRANSFER =74
	REQUEST_REPLICATION =74
	REQUEST_DB_FREEZE =74
	REQUEST_DB_RELEASE =74

	def connect(self):
		self.session_id=-1
		self.connection = Connection()
		self.sock = socket.create_connection(self.connection.get_url())
		self.protocol_version=self.read_short()
		assert self.protocol_version == 15

	def read_response(self):
		data = self.read_byte()
		sid = self.read_int()
		if ord(data)==0:
			return True
		else:
			self.read_error()
			return False

	def read_error(self):
		while True:
			following = self.read_byte()
			if ord(following)==0:
				break
			klass=self.read_string()
			message=self.read_string()

		

	def read_byte(self):
		return self.sock.recv(1)

	def write_byte(self,b):
		self.sock.send(struct.pack('!B',b))

	def read_short(self):
		data = self.sock.recv(2)
		return struct.unpack('!h',data)[0]

	def write_short(self,h):
		self.sock.send(struct.pack('!h',h))

	def read_int(self):
		data = self.sock.recv(4)
		return struct.unpack('!i',data)[0]

	def write_int(self,i):
		self.sock.send(struct.pack('!i',i))

	def read_long(self):
		data = self.sock.recv(8)
		return struct.unpack('!q',data)[0]

	def write_long(self,l):
		self.sock.send(struct.pack('!q',l))

	def read_bytes(self):
		length = self.read_int()
		if length>0:
			data = self.sock.recv(length)
			return struct.unpack('!'+str(length)+'s',data)[0]
		else:
			return None

	def write_bytes(self,b):
		length = len(b)
		self.sock.send(struct.pack('!'+str(length)+'s',b))

	def read_string(self):
		length = self.read_int()
		if(length>0):
			data=self.sock.recv(length)
			return struct.unpack('!'+str(length)+'s',data)[0]
		else:
			return None

	def write_string(self,s):
		length = len(s)
		self.write_int(self,length)
		self.sock.send(struct.pack('!'+str(length)+'s',s))

	def read_strings(self):
		length = self.read_int()
		s=[]
		for l in length:
			s.append(self.read_string())
		return s

	def write_strings(self,s):
		length = len(b)
		self.write_int(self,length)
		for st in s:
			self.write_string(st)

	def read_record(self):
		typ = self.read_short()
		if typ==0:
			record_type=self.read_byte()
			cluster_id=self.read_short()
			cluster_position=self.read_long()
			record_version=self.read_int()
			record_content=self.read_bytes()
		elif typ==-3:
			cluster_id=self.read_short()
			cluster_position=self.read_long()
			record_type=-3
			record_version=None
			record_content=None
		elif typ==-2:
			return {"typ":-2}

		return {"typ":typ,"record_type":record_type,"cluster_id":cluster_id,"cluster_position":cluster_position,"record_version":record_version,"record_content":record_content}


	def update_query(self,query,**kwargs):
		"""
		Update a send_command query with arguments taken from kwargs
		"""
		for i in range(len(query)):
			if query[i][0] in kwargs:
				tmp=list(query[i])
				tmp[1]=kwargs[tmp[0]]
				query[i]=tuple(tmp)


	def unpack_expected(self,content):
		"""
		Update a recv_command with expected data content
		"""
		result=[]
		for field in content:
			packmode=field[1]
			if packmode=="int":
				result.append((field[0],self.read_int()))
			elif packmode=="short":
				result.append((field[0],self.read_short()))
		return result


	def pack_content(self,content):
		"""
		Pack content for inclusion in a request
		TODO : add missing packmodes strings and record
		"""
		data=""
		for field in content:
			packmode="string" # default packing mode
			if len(field)>2:
				packmode=field[2]
			if packmode=="string":
				if field[1] is not None:
					data+=struct.pack('!i',len(field[1]))
					data+=struct.pack("!"+str(len(field[1]))+"s",field[1])
				else:
					data+=struct.pack('!i',-1)

			elif packmode=="short":
				data+=struct.pack("!h",field[1])
			elif packmode=="boolean":
				data+=struct.pack("!b",field[1])
			elif packmode=="byte":
				data+=struct.pack("!b",field[1])
			elif packmode=="int":
				data+=struct.pack("!i",field[1])
			elif packmode=="long":
				data+=struct.pack("!q",field[1])
			elif packmode=="bytes":
				if field[1] is not None:
					data+=struct.pack('!i',len(field[1]))
					data+=struct.pack("!"+str(len(field[1]))+"s",field[1])
				else:
					data+=struct.pack('!i',-1)

			elif packmode.startswith("list"): # for types list:subtype
				subtype=packmode[5:] # get subtype
				subpack=[]
				for f in field[1]:
					subpack.append(("",f,subtype))
				data+=struct.pack('!i',len(subpack))
				data+=self.pack_content(subpack)
			

		return data



	def send_request(self,command,content=None):
		"""
		Used by all send_command
		"""
		self.sock.send(struct.pack('!B',command))
		self.sock.send(struct.pack('!i',self.session_id))
		if content:
			self.write_bytes(content)


	def send_db_open(self,**kwargs):
		query=[
		("driver_name","orientdb python client"),
		("driver_version","0.1"),
		("protocol_version",15, "short"),
		("client_id","me"),
		("database_name","demo"),
		("database_type","document"),
		("user_name","ghpu"),
		("user_password","ghpu"),
		]
		self.update_query(query,**kwargs)
		packed=self.pack_content(query)
		self.send_request(self.DB_OPEN,packed)

	def recv_db_open(self):
		response=self.read_response()
		if not response:
			return
		expected=[
		("session_id","int"),
		("num_of_clusters","short"),
		]
		expected=self.unpack_expected(expected)
		self.session_id=expected[0][1]
		num_of_clusters=expected[1][1]
		clusters=[]
		for c in range(num_of_clusters):
			cluster_name=self.read_string()
			cluster_id=self.read_short()
			cluster_type=self.read_string()
			cluster_dataSegmentId=self.read_short()
		cluster_config=self.read_bytes()
		orientdb_release=self.read_string()

		return expected

	def send_shutdown(self,**kwargs):
		query=[
		("user_name",self.connection.user),
		("user_password",self.connection.password),
		]
		self.update_query(query,**kwargs)
		packed=self.pack_content(query)
		self.send_request(self.SHUTDOWN,packed)

	def send_connect(self,**kwargs):
		query=[
		("driver_name","orientdb python client"),
		("driver_version","0.1"),
		("protocol_version",15, "short"),
		("client_id","me"),
		("user_name","ghpu"),
		("user_password","ghpu"),
		]
		self.update_query(query,**kwargs)
		packed=self.pack_content(query)
		self.send_request(self.CONNECT,packed)

	def recv_connect(self,**kwargs):
		response=self.read_response()
		if not response:
			return
		expected=[
		("session_id","int"),
		]
		expected=self.unpack_expected(expected)
		self.session_id=expected[0][1]
		return expected


	def send_db_create(self,**kwargs):
		query=[
		("database_name","demo"),
		("database_type","document"),
		("storage_type","local"),
		]
		self.update_query(query,**kwargs)
		packed=self.pack_content(query)
		self.send_request(self.DB_CREATE,packed)

	def recv_db_create(self):
		pass

	def send_db_close(self):
		self.send_request(self.DB_CLOSE,None)

	def recv_db_close(self):
		pass

	def send_db_exist(self,**kwargs):
		query=[
		("database_name","demo"),
		]
		self.update_query(query,**kwargs)
		packed=self.pack_content(query)
		self.send_request(self.DB_EXIST,packed)

	def recv_db_exist(self):
		response=self.read_response()
		if not response:
			return
		result=self.sock.recv(1)
		if ord(result)==0:
			return False
		return True


	def send_db_reload(self,**kwargs):
		self.send_request(self.DB_RELOAD,None)

	def recv_db_reload(self,**kwargs):
		response=self.read_response()
		if not response:
			return
		expected=[
		("num_of_clusters","short"),
		("cluster_name","string"),
		("cluster_id","short"),
		("cluster_type","string"),
		("cluster_dataSegment","short"),
		]
		expected=self.unpack_expected(expected)
		return expected

	def send_db_drop(self,**kwargs):
		query=[
		("database_name","demo"),
		]
		self.update_query(query,**kwargs)
		packed=self.pack_content(query)
		self.send_request(self.DB_DROP,packed)

	def recv_db_drop(self):
		pass

	def send_db_size(self,**kwargs):
		self.send_request(self.DB_SIZE,None)

	def recv_db_size(self,**kwargs):
		response=self.read_response()
		if not response:
			return
		expected=[
		("size","long"),
		]
		expected=self.unpack_expected(expected)
		return expected[0][1]

	def recv_db_countrecords(self,**kwargs):
		response=self.read_response()
		if not response:
			return
		expected=[
		("count","long"),
		]
		expected=self.unpack_expected(expected)
		return expected[0][1]

	def send_datacluster_add(self,**kwargs):
		query=[
		("type","physical"),
		("name","cluster"),
		("location","local"),
		("datasegment_name","segment"),
		]
		self.update_query(query,**kwargs)
		packed=self.pack_content(query)
		self.send_request(self.DATACLUSTER_ADD,packed)

	def recv_datacluster_add(self):
		response=self.read_response()
		if not response:
			return
		expected=[
		("new_cluster","short"),
		]
		expected=self.unpack_expected(expected)
		return expected[0][1]

	def send_datacluster_remove(self,**kwargs):
		query=[
		("cluster_number",-1),
		]
		self.update_query(query,**kwargs)
		packed=self.pack_content(query)
		self.send_request(self.DATACLUSTER_REMOVE,packed)

	def recv_datacluster_remove(self):
		response=self.read_response()
		if not response:
			return
		delete_on_clientside = self.read_byte()
		if ord(delete_on_clientside)==1:
			return True
		else:
			return False

	def send_datacluster_count(self,**kwargs):
		query=[
		("cluster_count",0,"short"),
		("cluster_number",-1,"list:short"),
		]
		self.update_query(query,**kwargs)
		packed=self.pack_content(query)
		self.send_request(self.DATACLUSTER_COUNT,packed)

	def recv_datacluster_count(self):
		response=self.read_response()
		if not response:
			return
		records_in_clusters = self.read_long()
		return record_in_clusters

	def send_datacluster_datarange(self,**kwargs):
		query=[
		("cluster_number",-1,"short"),
		]
		self.update_query(query,**kwargs)
		packed=self.pack_content(query)
		self.send_request(self.DATACLUSTER_DATARANGE,packed)

	def recv_datacluster_count(self):
		response=self.read_response()
		if not response:
			return
		expected=[
		("begin","long"),
		("end","long"),
		]
		expected=self.unpack_expected(expected)
		return (expected[0][1],expected[1][1])

	def send_datasegment_add(self,**kwargs):
		query=[
		("name","cluster"),
		("location","local"),
		]
		self.update_query(query,**kwargs)
		packed=self.pack_content(query)
		self.send_request(self.DATASEGMENT_ADD,packed)

	def recv_datasegment_add(self):
		response=self.read_response()
		if not response:
			return
		expected=[
		("new_datasegment_id","int"),
		]
		expected=self.unpack_expected(expected)
		return expected[0][1]

	def send_datasegment_remove(self,**kwargs):
		query=[
		("name","cluster"),
		]
		self.update_query(query,**kwargs)
		packed=self.pack_content(query)
		self.send_request(self.DATASEGMENT_REMOVE,packed)

	def recv_datasegment_remove(self):
		response=self.read_response()
		if not response:
			return
		succeeded = self.read_byte()
		if ord(succeeded)==1:
			return True
		else:
			return False





	def send_record_load(self,**kwargs):
		query=[
		("cluster_id",-1,"short"),
		("cluster_position",1,"long"),
		("fetch_plan","*:0","string"),
		("ignore_cache",0,"byte"),
		("load_tombstone",0,"byte"),
		]
		self.update_query(query,**kwargs)
		packed=self.pack_content(query)
		self.send_request(self.RECORD_LOAD,packed)


	def recv_record_load(self):
		response=self.read_response()
		if not response:
			return

		records=[]
		while True:
			payload_status=self.read_byte()
			if ord(payload_status)==0:
				break
			elif ord(payload_status)==1:
				# a record for result
				record_content=self.read_bytes()
				record_version=self.read_int()
				record_type=self.read_byte()
				record={"record_content":record_content,"record_version":record_version,"record_type":record_type}
				records.append(record)
			time.sleep(10)
		return records

def print_hex(s):
	print ':'.join(x.encode('hex') for x in s)


def test():
	print Status.OK
	r=Request()
	r.connect()

#	r.send_connect()
#	r.recv_connect()

	r.send_db_open(database_name="demo")
	print "DB open : ",r.recv_db_open()

	#r.send_db_exist(database_name="demo")
	#print r.recv_db_exist()

	#r.send_db_drop(database_name="demode")
	#print r.recv_db_drop()

	#r.send_datacluster_count(cluster_count=1,cluster_number=[9])
	#print r.recv_datacluster_count()

	print "record load"
	r.send_record_load(cluster_id=5,cluster_position=2,fetch_plan="",ignore_cache=0)
	print r.recv_record_load()

#	r.send_db_reload()
#	r.send_db_close()


#	r.send_shutdown()
#	r.send_db_create()
#	r.send_db_close()
	time.sleep(10)


if __name__=="__main__":
	test()
