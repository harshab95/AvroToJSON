from confluent_kafka import KafkaError, Producer
from confluent_kafka.avro import AvroConsumer
from confluent_kafka.avro.serializer import SerializerError
import json as json
import os

contents=0
f=open("./offset.txt", "r")
offset=int(f.read())
f.close()

#need to add the hostname in Container

c = AvroConsumer({
    'bootstrap.servers': 'kafka1:9092',
    'group.id': offset, 
    'schema.registry.url': 'http://kafka1:8081',
    'auto.offset.reset': 'smallest'})

#RARS-PIAM is the avro topic
c.subscribe(['RARS_PIAM'])


#every deploment will have a 
f=open("./offset.txt", "w")
write_offset = str(offset + 1)
f.write(str(write_offset))
f.close()


#Producing JSON Message
p = Producer({'bootstrap.servers': 'kafka1:9092'})


while True:
    try:
        msg = c.poll(0.5)

    except SerializerError as e:
        print("Message deserialization failed for {}: {}".format(msg, e))
        break

    if msg is None:
        print("EMPTY MESSAGE")
        continue

    if msg.error():
        print("AvroConsumer error: {}".format(msg.error()))
        continue
   
    print(msg.value())
    encoded_msg = str(msg.value()).encode('utf-8')
    p.produce("RARS_EVENTS_JSON", encoded_msg)
    print("Message Produced!"+ str(i))
c.close()
