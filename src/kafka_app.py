from confluent_kafka import Consumer, KafkaError, KafkaException, Producer
import signal
import os
import socket

class KafkaApp:
    def __init__(self):
        self.consumer_conf = {'bootstrap.servers': 'localhost:9092',
                'group.id': 'timeseries-equalizer',
                'auto.offset.reset': 'smallest'}
        self.running = True
        signal.signal(signal.SIGINT, self.graceful_shutdown)
        signal.signal(signal.SIGTERM, self.graceful_shutdown)
        self.consumer = Consumer(self.consumer_conf)
        self.producer_conf = {'bootstrap.servers': 'localhost:9092',
                'client.id': socket.gethostname()}

        self.producer = Producer(self.producer_conf)

    def graceful_shutdown(self, signum, frame):
        self.running = False

    def run(self, topics, output_topic):
        try:
            print(f'Subscribing to topics: {topics}')
            self.consumer.subscribe(topics)

            while self.running:
                msg = self.consumer.poll(timeout=1.0)
                if msg is None:
                    continue

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        # End of partition event
                        print('No more messages')

                    elif msg.error():
                        raise KafkaException(msg.error())
                else:
                    msg_process(msg)
                    self.producer.produce(output_topic, value="value")
        finally:
            # Close down consumer to commit final offsets.
            self.consumer.close()

if __name__ == '__main__':
    input_topics = os.environ['SERVICE_INPUT_TOPICS'].split(',')
    output_topic = os.environ['SERVICE_OUTPUT_TOPIC']
    app = KafkaApp()
    app.run(input_topics, output_topic)
