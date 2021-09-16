import json
import time

from faker import *
from kafka import KafkaProducer


class DataGen():
    """
    DataGen for a fake iot device
    """
    def _get_fake_data(self, fake):
        """
        get the fake iot sensor data from faker
        """
        message = {
            'sensor_id': fake.pyint(min_value=1, max_value=3),
            'event_time': fake.date_time_between(start_date='-10m', end_date='now').isoformat(),
            'humidity': fake.pyint(min_value=20, max_value=100),
            'temperature': fake.pyint(min_value=20, max_value=50),
            'pressure': fake.pyint(min_value=60, max_value=100)
        }

        return json.dumps(message)


def main():
    """
    create a kafka producer and send the fake data to the topic
    a delay of 30 seconds is utilized
    """
    topic_name = 'agro'
    try:
        producer = KafkaProducer(bootstrap_servers='localhost:9092')
        fake = Faker()
        datagen = DataGen()

        while True:
            data = datagen._get_fake_data(fake)
            producer.send(topic_name, data.encode('utf-8'))
            time.sleep(30)
    except Exception as e:
        print(e)


if __name__ == '__main__':
    main()
