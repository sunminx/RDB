import redis
import unittest

class TestString(unittest.TestCase):
    def setUp(self):
        self.cli = redis.Redis(host="localhost", port=6379, decode_responses=True)

    def test_setget(self):
        key, val = "x", "foobar"
        self.cli.set(key, val)
        self.assertEqual(val, self.cli.get(key))

    def test_setdel(self):
        key, val = "y", "barfoo"
        self.cli.set(key, val)
        self.assertEqual(val, self.cli.get(key))
        self.cli.delete(key)
        self.cli.get(key)
        self.assertNotEqual(val, self.cli.get(key))

    def test_bigpayload(self):
        key, val = "foo", "abcd" * 1000000
        self.cli.set(key, val)
        self.assertEqual(val, self.cli.get(key))

    def test_randomaccess(self):
        import random

        payload = {}
        for i in range(1000):
            size = random.randint(1, 100000)
            key, val = f"bigpayload{i}", f"pl-{i}"*size
            payload[key] = val
            self.cli.set(key, val)
        for key, val in payload.items():
            self.assertEqual(val, self.cli.get(key))

    def test_numkey(self):
        for i in range(10000):
            self.cli.set(i, i)
        for i in range(9999, -1, -1):
            self.assertEqual(str(i), self.cli.get(i))

    def test_append(self):
        key, val = "x", "foo"
        self.cli.set(key, val)
        self.assertEqual(val, self.cli.get(key))
        val += "bar"
        self.cli.set(key, val)
        self.assertEqual(val, self.cli.get(key))

    def tearDown(self):
        if self.cli is not None:
            self.cli.close()

if __name__ == '__main__':
    unittest.main()
