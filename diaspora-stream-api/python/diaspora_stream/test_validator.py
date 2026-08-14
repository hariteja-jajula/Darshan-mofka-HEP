import unittest
from diaspora_stream.api import Validator, Exception


class TestValidator(unittest.TestCase):

    def test_create_default_validator(self):
        validator = Validator.from_metadata()
        self.assertIsInstance(validator, Validator)

    def test_create_default_validator_from_type(self):
        validator = Validator.from_metadata(type="default")
        self.assertIsInstance(validator, Validator)

    def test_create_schema_validator(self):
        schema = {
            "type": "object",
            "properties": {
                "name": {"type": "string"},
                "age": {"type": "number"}
            }
        }
        validator = Validator.from_metadata(type="schema", schema=schema)
        self.assertIsInstance(validator, Validator)

    def test_create_schema_validator_invalid_schema(self):
        with self.assertRaises(Exception):
            Validator.from_metadata(type="schema")
        with self.assertRaises(ValueError):
            Validator.from_metadata(type="schema", schema="not-an-object")

    def test_create_eventbridge_validator(self):
        schema = {
            "source": ["my.app"],
            "detail-type": ["my-event"]
        }
        validator = Validator.from_metadata(type="eventbridge", schema=schema)
        self.assertIsInstance(validator, Validator)

    def test_create_eventbridge_validator_invalid_schema(self):
        with self.assertRaises(Exception):
            Validator.from_metadata(type="eventbridge")
        with self.assertRaises(Exception):
            Validator.from_metadata(type="eventbridge", schema="not-an-object")

    def test_create_unknown_validator(self):
        with self.assertRaises(Exception):
            Validator.from_metadata(type="unknown")


if __name__ == '__main__':
    unittest.main()
