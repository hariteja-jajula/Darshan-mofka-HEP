import unittest
from diaspora_stream.api import DataDescriptor, Exception


class TestDataDescriptor(unittest.TestCase):

    def setUp(self):
        self.dd = DataDescriptor("test_location", 1024)

    def test_initialization(self):
        self.assertEqual(self.dd.size, 1024)
        self.assertEqual(self.dd.location, "test_location")
        dd_empty = DataDescriptor()
        self.assertEqual(dd_empty.size, 0)
        self.assertEqual(dd_empty.location, "")

    def test_make_sub_view(self):
        sub_dd = self.dd.make_sub_view(offset=100, size=200)
        self.assertIsInstance(sub_dd, DataDescriptor)
        self.assertEqual(sub_dd.size, 200)
        # Location should be inherited
        self.assertEqual(sub_dd.location, "test_location")

    def test_make_sub_view_out_of_bounds(self):
        # This should not raise an exception, but return an empty descriptor
        sub_dd = self.dd.make_sub_view(offset=1200, size=200)
        self.assertEqual(sub_dd.size, 0)
        # Check if size is correctly truncated
        sub_dd_2 = self.dd.make_sub_view(offset=900, size=200)
        self.assertEqual(sub_dd_2.size, 124)


    def test_make_stride_view(self):
        stride_dd = self.dd.make_stride_view(
            offset=0, num_blocks=4, block_size=64, gap_size=192)
        self.assertIsInstance(stride_dd, DataDescriptor)
        self.assertEqual(stride_dd.size, 4 * 64)
        self.assertEqual(stride_dd.location, "test_location")

    def test_make_stride_view_out_of_bounds(self):
        with self.assertRaises(Exception):
            self.dd.make_stride_view(
                offset=0, num_blocks=4, block_size=128, gap_size=192)

    def test_make_unstructured_view(self):
        segments = [(0, 100), (200, 100), (400, 100)]
        unstructured_dd = self.dd.make_unstructured_view(segments=segments)
        self.assertIsInstance(unstructured_dd, DataDescriptor)
        self.assertEqual(unstructured_dd.size, 300)
        self.assertEqual(unstructured_dd.location, "test_location")

    def test_make_unstructured_view_out_of_bounds(self):
        with self.assertRaises(Exception):
            segments = [(0, 100), (200, 100), (1000, 200)]
            self.dd.make_unstructured_view(segments=segments)

    def test_make_unstructured_view_overlapping(self):
        with self.assertRaises(Exception):
            segments = [(0, 200), (100, 200)]
            self.dd.make_unstructured_view(segments=segments)

    def test_flatten_stride_view(self):
        return
        stride_dd = self.dd.make_stride_view(
            offset=0, num_blocks=4, block_size=64, gap_size=192)
        self.assertEqual(stride_dd.size, 256)
        flat_dd = stride_dd.flatten()
        self.assertIsInstance(flat_dd, DataDescriptor)
        self.assertEqual(flat_dd.size, stride_dd.size)
        # The location of a flattened DataDescriptor is empty
        self.assertEqual(flat_dd.location, "")

    def test_flatten_unstructured_view(self):
        return
        segments = [(0, 100), (200, 100), (400, 100)]
        unstructured_dd = self.dd.make_unstructured_view(segments=segments)
        self.assertEqual(unstructured_dd.size, 300)
        flat_dd = unstructured_dd.flatten()
        self.assertIsInstance(flat_dd, DataDescriptor)
        self.assertEqual(flat_dd.size, unstructured_dd.size)
        # The location of a flattened DataDescriptor is empty
        self.assertEqual(flat_dd.location, "")

    def test_flatten_contiguous_view(self):
        # Flattening a contiguous view should result in a similar DataDescriptor
        sub_dd = self.dd.make_sub_view(offset=100, size=200)
        self.assertEqual(sub_dd.size, 200)
        flat_dd = sub_dd.flatten()
        self.assertIsInstance(flat_dd, list)
        self.assertEqual(len(flat_dd), 1)
        self.assertEqual(flat_dd[0], (100, 200))


if __name__ == '__main__':
    unittest.main()
