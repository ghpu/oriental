#!/usr/bin/env python
# -*- coding:utf-8 -*-

# Binary Protocol tentative for OrientDB 1.4

import os
import sys
import struct
import socket
from collections import OrderedDict

class Status:
	"""
	Contains an enum for all return status codes
	"""
	values = ['OK','DISCONNECTED','CONNECTIONFAILED']

	class __metaclass__(type):
		def __getattr__(self, name):
			print "Testing for :" ,name
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
		self.session_id = -1
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
		return struct.unpack('!l',data)[0]

	def write_long(self,l):
		self.sock.send(struct.pack('!l',l))

	def read_bytes(self):
		length = self.read_int()		
		data = self.sock.recv(length)
		return struct.unpack('!'+str(length)+'s',data)[0]

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

		return {"typ":typ,"record-type":record-type,"cluster-id":cluster-id,"cluster-position":cluster-position,"record-version":record-version,"record-content":record-content}


	def send_request(self,command,content=None):
		self.sock.send(struct.pack('!B',command))
		self.sock.send(struct.pack('!i',self.session_id))
		if content:
			self.write_bytes(content)


	def send_db_open(self):
		query=[
		("driver-name","orientdb python client"),
		("driver-version","0.1"),
		("protocol-version",15, "short"),
		("client-id","me"),
		("database-name","demo"),
		("database-type","document"),
		("user-name","ghpu"),
		("user-password","ghpu"),
		]
		packed=self.pack_content(query)
		self.send_request(self.DB_OPEN,packed)

	def recv_db_open(self):
		response=self.read_response()
		if not response:
			return
		expected=[
		("session-id","int"),
		("num-of-clusters","short"),
		]
		expected=self.unpack_expected(expected)
		self.session_id=expected[0][1]
		print "Session id is now : ",self.session_id

	def send_shutdown(self):
		query=[
		("user-name",self.connection.user),
		("user-password",self.connection.password),
		]
		packed=self.pack_content(query)
		self.send_request(self.SHUTDOWN,packed)

	def send_connect(self):
		query=[
		("driver-name","orientdb python client"),
		("driver-version","0.1"),
		("protocol-version",15, "short"),
		("client-id","me"),
		("user-name","ghpu"),
		("user-password","ghpu"),
		]
		packed=self.pack_content(query)
		self.send_request(self.CONNECT,packed)

	def recv_connect(self):
		response=self.read_response()
		if not response:
			return
		expected=[
		("session-id","int"),
		]
		expected=self.unpack_expected(expected)
		self.session_id=expected[0][1]
		print "Session id is now : ",self.session_id


	def send_db_create(self):
		query=[
		("database-name","demode"),
		("database-type","document"),
		("storage-type","local"),
		]
		packed=self.pack_content(query)
		self.send_request(self.DB_CREATE,packed)

	def recv_db_create(self):
		pass

	def send_db_close(self):
		self.send_request(self.DB_CLOSE,None)

	def recv_db_close(self):
		pass


	def unpack_expected(self,content):
		result=[]
		for field in content:
			packmode=field[1]
			if packmode=="int":
				result.append((field[0],self.read_int()))
			elif packmode=="short":
				result.append((field[0],self.read_short()))
		return result


	def pack_content(self,content):
		data=""
		for field in content:
			packmode="string" # default packing mode
			if len(field)>2:
				packmode=field[2]
			if packmode=="string":
				data+=struct.pack('!i',len(field[1]))
				data+=struct.pack("!"+str(len(field[1]))+"s",field[1])
			elif packmode=="short":
				data+=struct.pack("!h",field[1])
		return data



def test():
	print Status.OK
	r=Request()
	r.connect()
	#r.send_db_open()
	#r.recv_db_open()
	#r.send_shutdown()
	r.send_connect()
	r.recv_connect()
	r.send_db_create()
	r.send_db_close()


if __name__=="__main__":
	test()
