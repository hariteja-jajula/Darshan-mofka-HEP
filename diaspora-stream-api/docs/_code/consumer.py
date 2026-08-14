import sys
from diaspora_stream.api import Driver


def consume(root_path: str, topic_name: str):
    driver_options = {
        "root_path": root_path,
    }
    driver = Driver(backend="files", options=driver_options)
    topic = driver.open_topic(topic_name)
    consumer = topic.consumer(name="my_consumer")

    for i in range(0, 100):
        event = consumer.pull().wait(-1)
        print(event.metadata)
        if (i+1) % 10:
            event.acknowledge()


if __name__ == "__main__":

    if len(sys.argv) != 4:
        print(f"Usage: {sys.argv[0]} <root_path> <topic_name>")
    root_path = sys.argv[1]
    topic_name = sys.argv[2]
    consume(root_path, topic_name)
