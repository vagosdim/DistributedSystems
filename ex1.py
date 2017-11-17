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

MAX_MESSAGES = 4
nodesConnected = 0
peerList = []
lamportClocks = {}
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
		global peerCounter,peerList
		
		self.factory = factory
		self.no = int(no)
		self.updateCounter = 0
		peerList.append(self)
		

	def connectionMade(self):
		
		global nodesConnected,peerList
		self.connected = True
		
		#print "Connected to", self.transport.client
		try:
			self.transport.write('<connection up>,0,0,0')
		except Exception, e:
			print e.args[0]
		nodesConnected += 1
		print(nodesConnected)
		if(nodesConnected == 2 and self.no == 0):
			reactor.callLater(2, self.sendUpdate)

	def sendUpdate(self):
		
		global LC,MAX_MESSAGES,peerList
		if(self.updateCounter == MAX_MESSAGES):
			return

		print "Sending update"
		try:
			info = str(LC)+','+str(self.updateCounter)+','+str(self.no)		
			for peer in peerList:					
				peer.transport.write('<update'+str(self.updateCounter)+'>,'+info)
		except Exception, ex1:
			print "Exception trying to send: ", ex1.args[0]
		self.updateCounter += 1
		if self.connected == True:
			reactor.callLater(2, self.sendUpdate)

	def sendAck(self,no,counter):
		global LC
		print "sendAck"
		try:
			info = str(counter)+str(no)
			self.transport.write('<Ack>,'+str(LC)+','+info+','+str(self.no))
		except Exception, e:
			print e.args[0]

	def dataReceived(self, data):
		global LC,lamportClocks
		print 'Received ' + data
	
		tokens = data.split(',')
		LC = max(LC,int(tokens[1]))+1
		lamportClocks[self.no] = LC
		lamportClocks[int(tokens[3])] = int(tokens[1])
		
		if(data.startswith('<update')):
			self.sendAck(tokens[2],tokens[3])
			print('Data is <update>')
		elif(data.startswith('<Ack>')):
			self.acks += 1
			
			

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
