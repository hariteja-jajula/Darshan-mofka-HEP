import unittest
import os
import json
from diaspora_stream.api import Driver, Exception, TopicHandle, ThreadPool


class TestDriver(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        backend = os.environ.get("DIASPORA_TEST_BACKEND", "simple:libdiaspora-simple-backend.so")
        backend_args = json.loads(os.environ.get("DIASPORA_TEST_BACKEND_ARGS", "{}"))
        cls.driver = Driver(backend=backend, options=backend_args)

    @classmethod
    def tearDownClass(cls):
        del cls.driver

    def test_driver(self):
        self.assertIsInstance(self.driver, Driver)

    def test_driver_create_open_topic(self):
        topic_args = json.loads(os.environ.get("DIASPORA_TEST_TOPIC_ARGS", "{}"))
        self.assertFalse(self.driver.topic_exists("my_topic"))
        with self.assertRaises(Exception):
            self.driver.open_topic("my_topic")
        self.driver.create_topic("my_topic", options=topic_args)
        self.assertTrue(self.driver.topic_exists("my_topic"))
        topic = self.driver.open_topic("my_topic")
        self.assertIsInstance(topic, TopicHandle)

    def test_driver_default_thread_pool(self):
        pool = self.driver.default_thread_pool
        self.assertIsInstance(pool, ThreadPool)

    def test_driver_make_thread_pool(self):
        pool = self.driver.make_thread_pool(1)
        self.assertIsInstance(pool, ThreadPool)
        del pool

    def test_driver_list_topics(self):
        topic_args = json.loads(os.environ.get("DIASPORA_TEST_TOPIC_ARGS", "{}"))

        # Create multiple topics
        self.driver.create_topic("topic1", options=topic_args)
        self.driver.create_topic("topic2", options=topic_args)

        # List all topics
        topics = self.driver.list_topics()

        # Verify the result is a dictionary
        self.assertIsInstance(topics, dict)

        # Verify both topics are in the list
        self.assertIn("topic1", topics)
        self.assertIn("topic2", topics)

        # Check metadata for topic1
        topic1_metadata = topics["topic1"]
        self.assertIsInstance(topic1_metadata, dict)

        # Check that component JSON files are included (if the backend supports them)
        # The simple backend might not create these files, but the files driver should
        if "validator" in topic1_metadata:
            self.assertIsInstance(topic1_metadata["validator"], dict)

        if "serializer" in topic1_metadata:
            self.assertIsInstance(topic1_metadata["serializer"], dict)

        if "partition_selector" in topic1_metadata:
            self.assertIsInstance(topic1_metadata["partition_selector"], dict)

        # Check metadata for topic2
        topic2_metadata = topics["topic2"]
        self.assertIsInstance(topic2_metadata, dict)


if __name__ == '__main__':
    unittest.main()
