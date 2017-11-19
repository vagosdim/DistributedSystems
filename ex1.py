#
# Event-driven code that behaves as either a client or a server
# depending on the argument.  When acting as client, it connects 
# to a server and periodically sends an update message to it.  
# Each update is acked by the server.  When acting as server, it
# periodically accepts messages from connected clients.  Each
# message is followed by an acknowledgment.
#
# Tested with Python 2.7.8 and Twisted 14.0.2
#
import optparse

from twisted.internet.protocol import Protocol, ClientFactory
from twisted.internet import reactor
import time
import random
from time import sleep

MAX_MESSAGES = 5
nodesConnected = 0
peerList = []
#dictionary with keys:unique process id	  value:logical time
lamportClocks = {} 
#dictionary with keys:unique message id   value:number of acks received for msg
ackMessages = {}
#dictionary with keys:unique message id   value:message content
contentMessages = {}
#dictionary with all incoming messages. key:idt value:msg timestamp(TS)
messageTS = {} 
LC = 0

def parse_args():
	usage = """usage: %prog [options] process number [hostname]:port
	python peer.py 0 127.0.0.1:port """

	parser = optparse.OptionParser(usage)

	_, args = parser.parse_args()

	if len(args) != 2:
		print parser.format_help()
		parser.exit()

	processNo, addresses = args

	def parse_address(addr):
		if ':' not in addr:
			host = '127.0.0.1'
			port = addr
        	else:
			host, port = addr.split(':', 1)

		if not port.isdigit():
			parser.error('Ports must be integers.')

		return host, int(port)

	return processNo, parse_address(addresses)


class Peer(Protocol):

	acks = 0
	connected = False

	def __init__(self, factory,no):
		global peerCounter,peerList,LC
		
		self.factory = factory
		self.no = int(no)
		self.updateCounter = 1
		peerList.append(self)
		LC = self.no
		

	def connectionMade(self):
		
		global nodesConnected,peerList
		self.connected = True
		
		try:
			self.transport.write('<connection up>,0,0,0,')
		except Exception, e:
			print e.args[0]
		nodesConnected += 1
		print(nodesConnected)
		if(nodesConnected == 2 ):
			reactor.callLater(2, self.sendUpdate)

	def programStop(self):
		print('\n\n')
		print(contentMessages)	
		print('\n\n')
		print(ackMessages)
		sleep(1000)	

	#message format: <updateX>,LC,unique msg-ID
	def sendUpdate(self):
		
		global LC,MAX_MESSAGES,peerList,ackMessages,contentMessages,messageTS
		
		LC += self.no
		if(self.updateCounter == MAX_MESSAGES+1):	
			try:			
				for peer in peerList:					
					peer.transport.write('')
				if self.connected == True:
					reactor.callLater(2, self.sendUpdate)
			except Exception, ex1:
				print "Exception trying to send: ", ex1.args[0]
			return		
			#self.programStop()

		#print "Sending update"
		try:
			info = str(LC)+','+str(self.updateCounter)+','+str(self.no)+','
			idt =  str(self.updateCounter)+str(self.no)		
			idt = int(idt)
			
			ackMessages[idt] = 0
			message = '<Message from node'+str(self.no)+' message number:'
			message += str(self.updateCounter)+'>,'+info
			contentMessages[idt] = message
			messageTS[idt] = LC

			for peer in peerList:					
				peer.transport.write(message)
		except Exception, ex1:
			print "Exception trying to send: ", ex1.args[0]
		self.updateCounter += 1
		if self.connected == True:
			reactor.callLater(2, self.sendUpdate)

	#Ack format: <Ack>,LC,unique msg-ID,proccess ID who sends Ack
	def sendAck(self,idt):
		global LC
		print "sendAck"
		try:
			self.transport.write('<Ack>,'+str(LC)+','+idt+','+str(self.no)+',')
		except Exception, e:
			print e.args[0]

	def handleMessage(self,tokens):
		
		message1 = tokens[0]+','+tokens[1]+','+tokens[2]+','+tokens[3]
		message2 = tokens[4]+','+tokens[5]+','+tokens[6]+','+tokens[7]
		self.dataReceived(message1)
		self.dataReceived(message2)

	def dataReceived(self, data):
		global LC,lamportClocks,ackMessages,contentMessages,messageTS,peerList
		
		if(data == ''):
			return
		tokens = data.split(',')
		if(len(tokens)>5):
			self.handleMessage(tokens)
			return
		print(tokens)
		receivedLC = int(tokens[1])
		processNo = int(tokens[3]) 
		LC = max(LC,receivedLC)+1
		lamportClocks[self.no] = LC
		lamportClocks[processNo] = receivedLC

		#if(len(messageTS)>0):
		#	self.deliverMessages()
		
		if(data.startswith('<Message')):
			idt = str(tokens[2])+str(processNo)
			messageTS[idt] = receivedLC
			contentMessages[int(idt)] = data
			ackMessages[int(idt)] = 1
			#payload = 0.01
			for peer in peerList:				
				reactor.callLater(0.07,peer.sendAck,idt)
				#payload += 0.01
		elif(data.startswith('<Ack>')):
			idt = int(tokens[2])
			ackMessages[idt] += 1
			outputFile = open('delivered_messages_'+str(self.no),'a+')
			if(ackMessages[idt] == 2):
				outputFile.write(contentMessages[idt]+'\n')
				#contentMessages.pop(msg)
				#ackMessages.pop(msg)
				#messageTS.pop(msg)
				#print('\n\n\n\nENTERED\n\n\n\n')
			outputFile.close()
			
			self.acks += 1
			#self.deliverMessages()
			
	def deliverMessages(self):
		global ackMessages,contentMessages,messageTS
		#is a list of keys ascending order by value
		orderedMessages = sorted(messageTS,key=messageTS.get)
		outputFile = open('delivered_messages_'+str(self.no),'a+')
		for msg in orderedMessages:
			msg = int(msg)
			if(ackMessages[msg] == 2):
				outputFile.write(contentMessages[msg]+'\n')
				contentMessages.pop(msg)
				ackMessages.pop(msg)
				messageTS.pop(msg)
				print('\n\n\n\nENTERED\n\n\n\n')
			else:
				break
		outputFile.close()
		return

	def connectionLost(self, reason):
		print "Disconnected"
		self.connected = False
		self.done()

	def done(self):
		self.factory.finished(self.acks)


class PeerFactory(ClientFactory):

	def __init__(self, fname,no):
		print '@__init__'
		self.acks = 0
		self.fname = fname
		self.records = []
		self.no = no

	def finished(self, arg):
		self.acks = arg
		self.report()

	def report(self):
		print 'Received %d acks' % self.acks

	def clientConnectionFailed(self, connector, reason):
		print 'Failed to connect to:', connector.getDestination()
		self.finished(0)

	def clientConnectionLost(self, connector, reason):
		print 'Lost connection.  Reason:', reason

	def startFactory(self):
		print "@startFactory"
		self.fp = open(self.fname, 'w+')

	def stopFactory(self):
		print "@stopFactory"
		self.fp.close()

	def buildProtocol(self, addr):
		print "@buildProtocol"
		protocol = Peer(self,self.no)
		return protocol


if __name__ == '__main__':
	processNo, address = parse_args()


	if processNo == "0":
		factory = PeerFactory('log',processNo)
		reactor.listenTCP(2434, factory)
		print "Starting p0 @" + address[0] + " port " + str(address[1])	
		f = open("network.txt","w")
		f.write(str(address[0])+'\n')
		f.close()

	elif processNo == "1":
		factory0 = PeerFactory('log',processNo)
		host, port = address
		f = open("network.txt","a+")
		p0Address = f.readline()
		print "Connecting to host " + p0Address + " port " + str(port)
		reactor.connectTCP(p0Address, port, factory0)
		factory1 = PeerFactory('log',processNo)
		reactor.listenTCP(2434, factory1)
		f.write(str(address[0])+'\n')
		f.close()

	elif processNo == "2":
		factory0 = PeerFactory('log',processNo)
		factory1 = PeerFactory('log',processNo)
		host, port = address
		f = open("network.txt","r")
		p1Address = f.readline()
		p0Address = f.readline()
		print "Connecting to host " + p0Address + " port " + str(port)
		reactor.connectTCP(p0Address, port, factory0)	
		print "Connecting to host " + p1Address + " port " + str(port)
		reactor.connectTCP(p1Address, port, factory1)
		f.close()
	
	else:
		print "Error"	

reactor.run()
