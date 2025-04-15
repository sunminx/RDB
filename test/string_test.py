#!/usr/bin/python3

import redis
import unittest

class TestString(unittest.TestCase):
    def setUp(self):
        self.cli = redis.Redis(host="localhost", port=6379, decode_responses=True)

    def test_setget(self):
        key, val = "x", "foobar"
        self.cli.set(key, val)
        self.assertEqual(val, self.cli.get(key))

    def test_bigpayload(self):
        key, val = "foo", "abcd" * 1000000
        self.cli.set(key, val)
        self.assertEqual(val, self.cli.get(key))

    def tearDown(self):
        if self.cli is not None:
            self.cli.close()

if __name__ == '__main__':
    unittest.main()
