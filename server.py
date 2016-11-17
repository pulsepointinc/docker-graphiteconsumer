from kafka import KafkaConsumer
from os import environ
import socket

grapserver = environ["grapserver"] if "grapserver" in environ else "0.0.0.0"
grapport = environ["grapport"] if "grapport" in environ else 2003
topic = environ["topic"] if "topic" in environ else "mytopic"
bootstrap_servers = environ["bootstrap_servers"] if "bootstrap_servers" in environ else "localhost:9092"

consumer = KafkaConsumer(topic, bootstrap_servers=bootstrap_servers, group_id=None, max_partition_fetch_bytes=10485760, auto_offset_reset="earliest")

for msg in consumer:
	try:
		key, val, ts = msg.value.split(' ') 
	except ValueError:
    		print ("Value Failure")
	else:
		if ts.isdigit() and val.isdigit():
			print (key, val, ts)
			message = ('%s %s %s\n' % (key, val, ts))
			print ('sending message:\n%s' % message)
			sock = socket.socket()
			sock.connect((grapserver, grapport))
			sock.sendall(message)
			sock.close()
