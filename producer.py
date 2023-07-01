from kafka import KafkaProducer
import time

topic = 'log_analytics'
dataset = 'logfile.log'
batch = 10000

def delivery_callback(err, msg):
    if err:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))

p = KafkaProducer(bootstrap_servers='localhost:9092', api_version=(0,11,5), key_serializer=str.encode, retries=3)

try:
    with open(dataset, 'r') as f:
        batch_arr = []
        for idx, line in enumerate(f):
            batch_arr.append(line)
            value = " ".join(batch_arr).encode('utf-8')
            if len(batch_arr) > batch:
                p.send(topic, key=str(idx), value=value)
                p.flush()
            p.send(topic, key=str(idx), value=value)
            p.flush()
    p.flush()
    p.send(topic, key="Stop", value="Bye....")
except KeyboardInterrupt:
    pass

p.flush(30)