import unittest
from diaspora_stream.api import Serializer, Exception


class TestSerializer(unittest.TestCase):

    def test_create_default_serializer(self):
        serializer = Serializer.from_metadata()
        self.assertIsInstance(serializer, Serializer)

    def test_create_default_serializer_from_type(self):
        serializer = Serializer.from_metadata(type="default")
        self.assertIsInstance(serializer, Serializer)

    def test_create_schema_serializer(self):
        schema = {
            "type": "object",
            "properties": {
                "name": {"type": "string"},
                "age": {"type": "number"}
            }
        }
        serializer = Serializer.from_metadata(type="schema", schema=schema)
        self.assertIsInstance(serializer, Serializer)

    def test_create_schema_serializer_invalid_schema(self):
        with self.assertRaises(Exception):
            Serializer.from_metadata(type="schema")
        with self.assertRaises(ValueError):
            Serializer.from_metadata(type="schema", schema="not-an-object")

    def test_create_unknown_serializer(self):
        with self.assertRaises(Exception):
            Serializer.from_metadata(type="unknown")


if __name__ == '__main__':
    unittest.main()
