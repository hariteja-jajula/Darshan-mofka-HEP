import unittest
import threading
import time
import os
import json
import queue
from diaspora_stream.api import Driver, Exception as DiasporaException


class TestConcurrentProducerConsumer(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        backend = os.environ.get("DIASPORA_TEST_BACKEND", "simple:libdiaspora-simple-backend.so")
        backend_args = json.loads(os.environ.get("DIASPORA_TEST_BACKEND_ARGS", "{}"))
        cls.driver = Driver(backend=backend, options=backend_args)
        cls.topic_counter = 0

    @classmethod
    def tearDownClass(cls):
        del cls.driver

    def setUp(self):
        topic_args = json.loads(os.environ.get("DIASPORA_TEST_TOPIC_ARGS", "{}"))
        TestConcurrentProducerConsumer.topic_counter += 1
        self.topic_name = f"concurrent_topic_{time.time_ns()}_{self.topic_counter}"
        self.driver.create_topic(self.topic_name, options=topic_args)
        self.topic = self.driver.open_topic(self.topic_name)

    def tearDown(self):
        del self.topic

    def test_simultaneous_produce_consume(self):
        """Basic concurrent test with producer and consumer running simultaneously."""
        num_events = 50
        events_produced = {"count": 0}
        events_consumed = {"count": 0}
        producer_done = threading.Event()
        errors = queue.Queue()
        counter_lock = threading.Lock()

        def producer_worker():
            try:
                producer = self.topic.producer("producer")
                for i in range(num_events):
                    metadata = {"index": i}
                    producer.push(metadata).wait(timeout_ms=1000)
                    with counter_lock:
                        events_produced["count"] += 1
                producer.flush().wait(timeout_ms=5000)
                producer_done.set()
            except Exception as ex:
                errors.put(f"Producer error: {ex}")

        def consumer_worker():
            try:
                consumer = self.topic.consumer("consumer")
                start_time = time.time()
                timeout_seconds = 30

                while True:
                    with counter_lock:
                        if events_consumed["count"] >= num_events:
                            break
                    if time.time() - start_time > timeout_seconds:
                        errors.put("Consumer timeout")
                        break

                    future = consumer.pull()
                    event = None
                    while event is None:
                        if time.time() - start_time > timeout_seconds:
                            errors.put("Consumer timeout")
                            return
                        event = future.wait(timeout_ms=100)
                    if event.event_id is not None:
                        with counter_lock:
                            events_consumed["count"] += 1
                        event.acknowledge()
            except Exception as ex:
                errors.put(f"Consumer error: {ex}")

        # Start consumer first to ensure it's ready
        consumer_thread = threading.Thread(target=consumer_worker)
        consumer_thread.start()
        time.sleep(0.1)  # Small delay to ensure consumer is ready

        # Start producer
        producer_thread = threading.Thread(target=producer_worker)
        producer_thread.start()

        # Wait for both threads with timeout
        producer_thread.join(timeout=30)
        consumer_thread.join(timeout=30)

        # Check for errors
        if not errors.empty():
            self.fail(errors.get())

        self.assertEqual(events_produced["count"], num_events)
        self.assertEqual(events_consumed["count"], num_events)

    def test_producer_faster_than_consumer(self):
        """Test with producer faster than consumer, testing buffering behavior."""
        num_events = 30
        events_produced = {"count": 0}
        events_consumed = {"count": 0}
        producer_done = threading.Event()
        errors = queue.Queue()
        counter_lock = threading.Lock()

        def producer_worker():
            try:
                producer = self.topic.producer("producer")
                for i in range(num_events):
                    metadata = {"index": i, "data": f"event_{i}"}
                    producer.push(metadata).wait(timeout_ms=1000)
                    with counter_lock:
                        events_produced["count"] += 1
                producer.flush().wait(timeout_ms=5000)
                producer_done.set()
            except Exception as ex:
                errors.put(f"Producer error: {ex}")

        def consumer_worker():
            try:
                consumer = self.topic.consumer("consumer")
                start_time = time.time()
                timeout_seconds = 30

                while True:
                    with counter_lock:
                        if events_consumed["count"] >= num_events:
                            break
                    if time.time() - start_time > timeout_seconds:
                        errors.put("Consumer timeout")
                        break

                    future = consumer.pull()
                    event = None
                    while event is None:
                        if time.time() - start_time > timeout_seconds:
                            errors.put("Consumer timeout")
                            return
                        event = future.wait(timeout_ms=100)
                    if event.event_id is not None:
                        with counter_lock:
                            events_consumed["count"] += 1
                        event.acknowledge()
                        # Simulate slow consumer
                        time.sleep(0.01)
            except Exception as ex:
                errors.put(f"Consumer error: {ex}")

        # Start consumer first
        consumer_thread = threading.Thread(target=consumer_worker)
        consumer_thread.start()
        time.sleep(0.1)

        # Start producer
        producer_thread = threading.Thread(target=producer_worker)
        producer_thread.start()

        # Wait for both threads with timeout
        producer_thread.join(timeout=30)
        consumer_thread.join(timeout=30)

        # Check for errors
        if not errors.empty():
            self.fail(errors.get())

        self.assertEqual(events_produced["count"], num_events)
        self.assertEqual(events_consumed["count"], num_events)

    def test_multiple_producers_single_consumer(self):
        """Test thread-safety with multiple producer threads."""
        num_producers = 3
        events_per_producer = 20
        total_events = num_producers * events_per_producer
        events_produced = {"count": 0}
        events_consumed = {"count": 0}
        producers_done = {"count": 0}
        errors = queue.Queue()
        counter_lock = threading.Lock()

        def producer_worker(producer_id):
            try:
                producer = self.topic.producer(f"producer_{producer_id}")
                for i in range(events_per_producer):
                    metadata = {"producer": producer_id, "index": i}
                    producer.push(metadata).wait(timeout_ms=1000)
                    with counter_lock:
                        events_produced["count"] += 1
                producer.flush().wait(timeout_ms=5000)
                with counter_lock:
                    producers_done["count"] += 1
            except Exception as ex:
                errors.put(f"Producer {producer_id} error: {ex}")

        def consumer_worker():
            try:
                consumer = self.topic.consumer("consumer")
                start_time = time.time()
                timeout_seconds = 30

                while True:
                    with counter_lock:
                        if events_consumed["count"] >= total_events:
                            break
                    if time.time() - start_time > timeout_seconds:
                        errors.put("Consumer timeout")
                        break

                    future = consumer.pull()
                    event = None
                    while event is None:
                        if time.time() - start_time > timeout_seconds:
                            errors.put("Consumer timeout")
                            return
                        event = future.wait(timeout_ms=100)
                    if event.event_id is not None:
                        with counter_lock:
                            events_consumed["count"] += 1
                        event.acknowledge()
            except Exception as ex:
                errors.put(f"Consumer error: {ex}")

        # Start consumer first
        consumer_thread = threading.Thread(target=consumer_worker)
        consumer_thread.start()
        time.sleep(0.1)

        # Start multiple producers
        producer_threads = []
        for p in range(num_producers):
            t = threading.Thread(target=producer_worker, args=(p,))
            producer_threads.append(t)
            t.start()

        # Wait for all threads with timeout
        for t in producer_threads:
            t.join(timeout=30)
        consumer_thread.join(timeout=30)

        # Check for errors
        if not errors.empty():
            self.fail(errors.get())

        self.assertEqual(events_produced["count"], total_events)
        self.assertEqual(events_consumed["count"], total_events)
        self.assertEqual(producers_done["count"], num_producers)


if __name__ == '__main__':
    unittest.main()
