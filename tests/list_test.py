import redis
import unittest

class TestList(unittest.TestCase):
    def setUp(self):
        self.cli = redis.Redis(host="localhost", port=6379, decode_responses=True)
        return

    def test_push_pop(self):
        key = "list1"
        items = ["a", "b", "c", "d"]
        size = 10000
        for item in items:
            self.cli.lpush(key, item * size)
        for item in reversed(items):
            self.assertEqual(self.cli.lpop(key), item * size)
        self.cli.flushall()

    def tearDown(self):
        if self.cli is not None:
            self.cli.close()
