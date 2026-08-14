import sys
from diaspora_stream.api import Driver


def main(root_path: str):
    options = {
        "root_path": root_path,
    }
    driver = Driver(backend="files", options=options)

    # START CREATE TOPIC
    from diaspora_stream.api import Validator, PartitionSelector, Serializer

    validator = Validator.from_metadata(
        type="energy_validator:libenergy_validator.so", energy_max=100)
    selector = PartitionSelector.from_metadata(
        type="energy_partition_selector:libenergy_partition_selector.so", energy_max=100)
    serializer = Serializer.from_metadata(
        type="energy_serializer:libenergy_serializer.so", energy_max=100)

    driver.create_topic(
        name="collisions",
        validator=validator,
        partition_selector=selector,
        serializer=serializer)
    # END CREATE TOPIC

    # START PRODUCER
    from diaspora_stream.api import ThreadPool, AdaptiveBatchSize, Ordering

    topic = driver.open_topic("collisions")
    thread_pool = driver.make_thread_pool(4)
    batch_size = AdaptiveBatchSize # or an integer > 0
    max_num_batches = 2
    ordering = Ordering.Strict # or Ordering.Loose

    producer = topic.producer(
        name="app1",
        batch_size=batch_size,
        max_num_batches=max_num_batches,
        thread_pool=thread_pool,
        ordering=ordering)
    # END PRODUCER

    # START EVENT
    # data can be any object adhering to the buffer protocol
    data1 = b"abcd"
    data2 = bytearray("efgh", encoding="ascii")
    data3 = memoryview(data1)
    # or a list of such objects
    data4 = [data1, data2, data3]

    metadata1 = """{"energy": 42}""" # use a string
    metadata2 = {"energy": 42} # or use a dictionary
    # END EVENT

    # START PRODUCE EVENT
    future = producer.push(metadata=metadata1, data=data1)
    future.completed # returns True if the future has completed
    event_id = future.wait()

    producer.flush().wait(-1)
    # END PRODUCE EVENT

    # START CONSUMER
    from diaspora_stream.api import ThreadPool, AdaptiveBatchSize, DataDescriptor

    batch_size = AdaptiveBatchSize
    thread_pool = driver.make_thread_pool(0)

    def data_selector(metadata, descriptor):
        if metadata["energy"] > 20:
            return descriptor
        else:
            return None

    def data_allocator(metadata, descriptor):
        # note that we return a *list* of objects satisfying the buffer protocol
        return [ bytearray(descriptor.size) ]

    consumer = topic.consumer(
        name="app2",
        thread_pool=thread_pool,
        batch_size=batch_size,
        data_selector=data_selector,
        data_allocator=data_allocator)
    # END CONSUMER

    # START CONSUME EVENTS
    future = consumer.pull()
    future.completed # returns true if the future has completed

    event    = future.wait(-1)
    data     = event.data
    metadata = event.metadata
    event_id = event.event_id

    event.acknowledge()
    # END CONSUME EVENTS

if __name__ == "__main__":

    if len(sys.argv) != 2:
        print(f"Usage: {sys.argv[0]} <root_path>")
    root_path = sys.argv[1]
    main(root_path)
