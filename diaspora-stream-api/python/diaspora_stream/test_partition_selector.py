import unittest
from diaspora_stream.api import PartitionSelector, Exception


class TestPartitionSelector(unittest.TestCase):

    def test_create_default_partition_selector(self):
        selector = PartitionSelector.from_metadata()
        self.assertIsInstance(selector, PartitionSelector)

    def test_create_default_partition_selector_from_type(self):
        selector = PartitionSelector.from_metadata(type="default")
        self.assertIsInstance(selector, PartitionSelector)

    def test_create_unknown_partition_selector(self):
        with self.assertRaises(Exception):
            PartitionSelector.from_metadata(type="unknown")


if __name__ == '__main__':
    unittest.main()
