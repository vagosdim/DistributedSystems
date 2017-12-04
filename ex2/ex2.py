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

MAX_MESSAGES = 5
NODES_CONNECTED = 0
peerList = []
decision = ''
vote = ''
state = 'INIT'
log = []
receivedVotes = 0
receivedCommits = 0
timeout = False

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
		

	def connectionMade(self):
		
		global NODES_CONNECTED,peerList
		self.connected = True
		
		try:
			self.transport.write('<Connection up>')
		except Exception, e:
			print e.args[0]
		NODES_CONNECTED += 1
		if(NODES_CONNECTED == 2 and self.no == 0):
			reactor.callLater(3, self.sendRequest)
			#reactor.callLater(2, self.sendUpdate)

	def multicast(self,data):
		for peer in peerList:					
			peer.transport.write(data)
		
	def sendRequest(self):
		
		global startTime,MAX_MESSAGES,peerList,timeout
		timeout = False
		
		if(self.updateCounter == 3):	
			#self.waitForVotes()
			return		

		try:
			print('\nMulticasting Message\n')
			message = 'VOTE_REQUEST,from node:'+str(self.no)+',request number:'
			message += str(self.updateCounter)
			self.multicast(message)
		except Exception, ex1:
			print "Exception trying to send: ", ex1.args[0]
		self.updateCounter += 1
		startTime = time.time()
		self.waitForVotes()
		#if self.connected == True:
			#reactor.callLater(5, self.sendRequest)

	def makeDecision(self):
		global receivedCommits,decision
		if(receivedCommits == 2):
			decision = 'GLOBAL_COMMIT'
		else:
			decision = 'GLOBAL_ABORT'
		state = decision
		self.writeToLog(decision)
		self.multicast(decision)
			

	def waitForVotes(self):

		global receivedVotes,receivedCommits,startTime,timeout
		
		if(receivedVotes < 2):
			if(time.time()-startTime > 3):
				timeout = True
				print('TIMEOUT.Answers not received within 3 seconds')
				state = 'GLOBAL_ABORT'
				self.writeToLog(state)
				self.multicast(state)
				reactor.callLater(3, self.sendRequest)
			else:
				reactor.callLater(2, self.waitForVotes)
		else:
			self.makeDecision()
			receivedVotes = 0
			receivedCommits = 0
			reactor.callLater(3, self.sendRequest)
		return

	def sendState(self,data):
		#global 
		print "send current state"
		try:
			self.transport.write(str(data)+',Node'+str(self.no))
		except Exception, e:
			print e.args[0]

	def writeToLog(self,data):
		global log,state
		f = open('log'+str(self.no),'a+')
		f.write(data+'\n')
		if(data.endswith('\n')):
			fields = data.split('\n')
			data = fields[0]
		log.append(data)
		print(log)
		state = data
		f.close()
		return

	def makeVote(self):
		decision = random.randint(0,1)
		if(decision == 0):
			return 'VOTE_COMMIT'
		else:
			return 'VOTE_ABORT'
		
		
	def dataReceived(self, data):
		global vote,state,log,startTime,receivedCommits,receivedVotes,timeout
		if(timeout == True):
			receivedCommits = 0
			receivedVotes = 0
			return
			 
		print('Received: '+data)
		if(data == '' or data == '<Connection up>'):
			return

		tokens = data.split(',')
		if(tokens[0] == 'VOTE_REQUEST'):
			vote = self.makeVote()
			self.writeToLog(vote)
			reactor.callLater(2,self.sendState,state)
			#if(vote == 'VOTE_COMMIT'):				
			#	startTime = time.time()
		#		self.waitFunction()
		elif(tokens[0] == 'GLOBAL_COMMIT' or tokens[0] == 'GLOBAL_ABORT'):
			self.writeToLog(tokens[0]+'\n')
		elif(tokens[0] == 'VOTE_COMMIT'):
			receivedCommits += 1
			receivedVotes += 1
		elif(tokens[0] == 'VOTE_ABORT'):
			receivedVotes += 1
		#if(tokens[0] == 'DECISION_REQUEST'):
		#	if(state == 'GLOBAL_COMMIT'):
		#		self.sendAck(state)
		#	elif(state == 'GLOBAL_ABORT' or state == 'INIT'):
		#		self.sendAck(state)
		#	else:
		#		return
			

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
