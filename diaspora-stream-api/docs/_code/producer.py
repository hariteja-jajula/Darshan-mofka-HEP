import sys
from diaspora_stream.api import Driver


def produce(root_path: str, topic_name: str):
    driver_options = {
        "root_path": root_path,
    }
    driver = Driver(backend="files", options=driver_options)
    topic = driver.open_topic(topic_name)
    producer = topic.producer()

    for i in range(0, 100):
        future = producer.push(
            metadata={"x": i*42, "name": "bob"},
            data=bytes())
        event_id = future.wait()


if __name__ == "__main__":

    if len(sys.argv) != 3:
        print(f"Usage: {sys.argv[0]} <root_path> <topic_name>")
    root_path = sys.argv[1]
    topic_name = sys.argv[2]
    produce(root_path, topic_name)
