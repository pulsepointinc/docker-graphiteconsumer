# docker-graphiteconsumer

Run server.py inside a docker container:
- open a socket to graphite server and port
- consume metrics.kafka topic from Kafka
- split each message and send to graphite!

The env variables referenced within server.py they are set in Marathon, see repo dm-apps.
