import logging
import socket
from os import environ

from kafka import KafkaConsumer


def get_env_config(var, default):
    return environ[var] if var in environ else default

grapserver = get_env_config("grapserver", "0.0.0.0")
grapport = int(get_env_config("grapport", 2003))
topic = get_env_config("topic", "mytopic")
bootstrap_servers = get_env_config("bootstrap_servers", "localhost:9092")
group_id = get_env_config("group_id", None)
max_partition_fetch_bytes = int(get_env_config("max_partition_fetch_bytes", 10485760))
auto_offset_reset = get_env_config("auto_offset_reset", "earliest")
loglevel = get_env_config("loglevel", "INFO")

conf = {
    "bootstrap_servers": bootstrap_servers,
    "group_id": group_id,
    "max_partition_fetch_bytes": max_partition_fetch_bytes,
    "auto_offset_reset": auto_offset_reset,
}

level = getattr(logging, loglevel.upper())
logging.basicConfig(level=level)
logging.info("Creating KafkaConsumer for topic(s) {} with config {}".format(topic, str(conf)))
consumer = KafkaConsumer(topic, **conf)

for msg in consumer:
    try:
        key, val, ts = msg.value.split(' ') 
    except:
        logging.exception("Failed in extracting metric from {}".format(str(msg)))
    else:
        if ts.isdigit() and val.isdigit():
            message = ("{} {} {}\n".format(key, val, ts))
            logging.debug("Sending message: {}\n".format(message))
            sock = socket.socket()
            sock.connect((grapserver, grapport))
            sock.sendall(message)
            sock.close()
