import unittest
import os
import json
from diaspora_stream.api import Driver, Exception, TopicHandle, ThreadPool


class TestTopicHandle(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        backend = os.environ.get("DIASPORA_TEST_BACKEND", "simple:libdiaspora-simple-backend.so")
        backend_args = json.loads(os.environ.get("DIASPORA_TEST_BACKEND_ARGS", "{}"))
        topic_args = json.loads(os.environ.get("DIASPORA_TEST_TOPIC_ARGS", "{}"))
        cls.driver = Driver(backend=backend, options=backend_args)
        cls.driver.create_topic("my_topic", options=topic_args)
        cls.topic = cls.driver.open_topic("my_topic")

    @classmethod
    def tearDownClass(cls):
        del cls.topic
        del cls.driver

    def test_topic_handle(self):
        self.assertIsInstance(self.topic, TopicHandle)
        self.assertEqual(self.topic.name, "my_topic")
        partitions = self.topic.partitions
        self.assertEqual(len(partitions), 1)

if __name__ == '__main__':
    unittest.main()
