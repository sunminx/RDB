import redis
import unittest

class TestString(unittest.TestCase):
    def setUp(self):
        self.cli = redis.Redis(host="localhost", port=6379, decode_responses=True)

    def test_setget(self):
        key, val = "x", "foobar"
        self.cli.set(key, val)
        self.assertEqual(val, self.cli.get(key))
        self.cli.flushall()

    def test_setgetempty(self):
        key, val = "x", ""
        self.cli.set(key, val)
        self.assertEqual(val, self.cli.get(key))
        self.cli.flushall()

    def test_setdel(self):
        key, val = "y", "barfoo"
        self.cli.set(key, val)
        self.assertEqual(val, self.cli.get(key))
        self.cli.delete(key)
        self.assertNotEqual(val, self.cli.get(key))
        self.cli.flushall()

    def test_bigpayload(self):
        key, val = "foo", "abcd" * 1000000
        self.cli.set(key, val)
        self.assertEqual(val, self.cli.get(key))
        self.cli.flushall()

    def test_randomaccess(self):
        import random

        payload = {}
        for i in range(1000):
            size = random.randint(1, 10000)
            key, val = f"bigpayload{i}", f"pl-{i}"*size
            payload[key] = val
            self.cli.set(key, val)
        for key, val in payload.items():
            self.assertEqual(val, self.cli.get(key))
        self.cli.flushall()

    def test_numkey(self):
        self.cli.flushall()
        for i in range(10000):
            self.cli.set(i, i)
        for i in range(9999, -1, -1):
            self.assertEqual(str(i), self.cli.get(i))
        # check db size before flush
        self.assertEqual(10000, self.cli.dbsize())
        self.cli.flushall()

    def test_setnx_keymissing(self):
        key, val = "novar", "foobared"
        self.cli.delete(key)
        self.assertEqual(1, self.cli.setnx(key, val))
        self.assertEqual(val, self.cli.get(key))
        self.cli.flushall()

    def test_setnx_keyexists(self):
        key, val = "novar", "foobared"
        self.cli.set(key, val)
        self.assertEqual(0, self.cli.setnx(key, val))
        self.assertEqual(val, self.cli.get(key))
        self.cli.flushall()

    def test_setnx_not_expired_key(self):
        key, val = "x", 10
        self.cli.set(key, val)
        self.cli.expire(key, 10000)
        self.assertEqual(0, self.cli.setnx(key, 20))
        self.assertEqual("10", self.cli.get(key))
        self.cli.flushall()

    def test_setnx_againest_expiried_volatile_key(self):
        import time

        for i in range(10000):
            self.cli.setex(f'key-{i}', 3600, "value")

        self.cli.set("key", 10)
        self.cli.expire("key", 1)

        time.sleep(2)

        self.assertEqual(1, self.cli.setnx("key", 20))
        self.assertEqual("20", self.cli.get("key"))
        self.cli.flushall()

    def test_mget(self):
        key1, key2 = "foo", "bar"
        val1, val2 = "BAR", "FOO"
        self.cli.set(key1, val1)
        self.cli.set(key2, val2)
        self.assertEqual([val1, val2], self.cli.mget([key1, key2]))
        self.cli.flushall()

    def test_mget_not_exists_key(self):
        key1, key2 = "foo", "bar"
        val1, val2 = "BAR", "FOO"
        self.cli.set(key1, val1)
        self.cli.set(key2, val2)
        self.assertEqual([val1, None, val2], self.cli.mget([key1, "key", key2]))
        self.cli.flushall()

    def test_append(self):
        key, val = "x", "foo"
        self.cli.set(key, val)
        self.assertEqual(val, self.cli.get(key))
        val += "bar"
        self.cli.set(key, val)
        self.assertEqual(val, self.cli.get(key))
        self.cli.flushall()

    def test_mset(self):
        k1, k2, k3 = "x", "y", "z"
        v1, v2, v3 = "10", "foobar", "x x x x x x x\\n\\n\\r\\n"
        self.cli.mset({k1: v1, k2: v2, k3: v3})
        self.assertEqual([v1, v2, v3], self.cli.mget([k1, k2, k3]))
        self.cli.flushall()

    def test_mset_wrong_number_keys(self):
        k1, k2, k3 = "x", "y", "z"
        v1, v2 = "10", "foobar"
        try:
            self.cli.execute_command("MSET", k1, v1, k2, v2, k3)
        except Exception as e:
            self.assertRegex(str(e), r"wrong number")
        self.cli.flushall()

    def test_msetnx_exists_key(self):
        k1, k2, k3 = "x", "y", "z"
        v1, v2, v3 = "10", "foobar", "x x x x x x x\\n\\n\\r\\n"
        self.cli.set(k2, v2)
        self.assertEqual(0, self.cli.msetnx({k1:v1, k2:v2, k3:v3}))
        self.assertEqual(None, self.cli.get(k1))
        self.assertEqual(None, self.cli.get(k3))
        self.cli.flushall()

    def test_msetnx_not_exists_key(self):
        k1, k2 = "x1", "x2"
        v1, v2 = "xxx", "yyy"
        self.assertEqual(1, self.cli.msetnx({k1:v1, k2:v2}))
        self.assertEqual(v1, self.cli.get(k1))
        self.assertEqual(v2, self.cli.get(k2))
        self.cli.flushall()

    def test_non_exists_key(self):
        self.assertEqual(0, self.cli.strlen("notakey"))
        self.cli.flushall()

    def test_int_encoded_value(self):
        k, v = "myint", -555
        self.cli.set(k, v)
        self.assertEqual(4, self.cli.strlen(k))
        self.cli.flushall()

    def test_plain_str_value(self):
        k, v = "mystr", "foozzz0123456789 baz"
        self.cli.set(k, v)
        self.assertEqual(20, self.cli.strlen(k))
        self.cli.flushall()

    def test_setbit_non_existing_key(self):
        k = "mykey"
        self.cli.delete(k)
        self.assertEqual(0, self.cli.setbit(k, 2, 1))
        self.assertEqual(1, self.cli.getbit(k, 2))
        self.assertEqual('00100000', self.to_bit(self.cli.get(k)))
        self.cli.flushall()

    def test_setbit_againest_string_encoded_key(self):
        # @ 64 01000000
        k, v = "mykey", "@"
        self.cli.set(k, v)
        self.assertEqual(0, self.cli.setbit(k, 2, 1))
        self.assertEqual('01100000', self.to_bit(self.cli.get(k)))
        self.assertEqual(1, self.cli.setbit(k, 1, 0))
        self.assertEqual('00100000', self.to_bit(self.cli.get(k)))
        self.cli.flushall()

    def test_setbit_againest_integer_encoded_key(self):
        # 1 49 00110001
        k, v = "mykey", 1
        self.cli.set(k, v)
        self.assertEqual(0, self.cli.setbit(k, 6, 1))
        self.assertEqual('00110011', self.to_bit(self.cli.get(k)))
        self.assertEqual(1, self.cli.setbit(k, 2, 0))
        self.assertEqual('00010011', self.to_bit(self.cli.get(k)))
        self.cli.flushall()

    def test_setbit_againest_wrong_type_key(self):
        k = "mylist"
        self.cli.delete(k)
        self.cli.lpush(k, "foo")
        try:
            self.cli.setbit(k, 0, 1)
        except Exception as e:
            self.assertRegex(str(e), "WRONGTYPE")
        self.cli.flushall()

    def test_setbit_with_out_of_range_offset(self):
        k = "mykey"
        try:
            self.cli.setbit(k, 4*1024*1024*1024, 1)
        except Exception as e:
            self.assertRegex(str(r), "out of range")
        self.cli.flushall()

    def test_setbit_with_out_of_range_offset(self):
        k = "mykey"
        bs = [-1, 2, 10, 20]
        for b in bs:
            try:
                self.cli.setbit(k, 0, b)
            except Exception as e:
                self.assertRegex(str(r), "out of range")
        self.cli.flushall()

    def test_setbit_fuzzing(self):
        import random

        size = 256
        ln = size*8
        k = "mykey"
        for i in range(1):
            v = "\0"*size
            self.cli.set(k, v)
            bit_offset = random.randint(0, ln-1)
            bit_offset = random.randint(0, ln-1)
            bit_value = random.randint(0, 1)
            bit_list = list(self.to_bit(v))
            bit_list[bit_offset] = str(bit_value)
            new_v = self.to_str(''.join(bit_list))
            self.assertEqual(0, self.cli.setbit(k, bit_offset, bit_value))
            self.assertEqual(new_v, self.cli.get(k))
        self.cli.flushall()

    def test_getbit_againest_non_existing_key(self):
        k = "mykey"
        self.cli.delete(k)
        self.assertEqual(0, self.cli.getbit(k, 0))
        self.cli.flushall()

    def test_getbit_againest_string_encoded_key(self):
        # ` 96 01100000
        k, v = "mykey", "`"
        self.cli.set(k, v)
        self.assertEqual(0, self.cli.getbit(k, 0))
        self.assertEqual(1, self.cli.getbit(k, 1))
        self.assertEqual(1, self.cli.getbit(k, 2))
        self.assertEqual(0, self.cli.getbit(k, 3))
        self.assertEqual(0, self.cli.getbit(k, 8))
        self.assertEqual(0, self.cli.getbit(k, 100))
        self.assertEqual(0, self.cli.getbit(k, 1000))
        self.cli.flushall()

    def test_getbit_againest_integer_encoded_key(self):
        # 1 49 00110000
        k, v = "mykey", 1
        self.cli.set(k, v)
        self.assertEqual(0, self.cli.getbit(k, 0))
        self.assertEqual(0, self.cli.getbit(k, 1))
        self.assertEqual(1, self.cli.getbit(k, 2))
        self.assertEqual(1, self.cli.getbit(k, 3))
        self.assertEqual(0, self.cli.getbit(k, 8))
        self.assertEqual(0, self.cli.getbit(k, 100))
        self.assertEqual(0, self.cli.getbit(k, 1000))
        self.cli.flushall()

    def test_setrange_againest_non_existing_key(self):
        k = "mykey"
        self.cli.delete(k)
        self.assertEqual(3, self.cli.setrange(k, 0, "foo"))
        self.assertEqual("foo", self.cli.get(k))
        self.cli.delete(k)
        self.assertEqual(0, self.cli.setrange(k, 0, ""))
        self.assertEqual(None, self.cli.get(k))
        self.cli.delete(k)
        self.assertEqual(4, self.cli.setrange(k, 1, "foo"))
        self.assertEqual("\0foo", self.cli.get(k))
        self.cli.flushall()

    def test_setrange_againest_string_encoded_value(self):
        k, v = "mykey", "foo"
        self.cli.set(k, v)
        self.assertEqual(3, self.cli.setrange(k, 0, "b"))
        self.assertEqual("boo", self.cli.get(k))

        self.cli.set(k, v)
        self.assertEqual(3, self.cli.setrange(k, 0, ""))
        self.assertEqual("foo", self.cli.get(k))

        self.cli.set(k, v)
        self.assertEqual(3, self.cli.setrange(k, 1, "b"))
        self.assertEqual("fbo", self.cli.get(k))

        self.cli.set(k, v)
        self.assertEqual(7, self.cli.setrange(k, 4, "bar"))
        self.assertEqual("foo\0bar", self.cli.get(k))
        self.cli.flushall()

    def test_setrange_againest_integer_encoded_value(self):
        k, v = "mykey", 1234
        self.cli.set(k, v)
        self.assertEqual(4, self.cli.setrange(k, 0, 2))
        self.assertEqual("2234", self.cli.get(k))

        self.cli.set(k, v)
        self.assertEqual(4, self.cli.setrange(k, 0, ""))
        self.assertEqual("1234", self.cli.get(k))

        self.cli.set(k, v)
        self.assertEqual(4, self.cli.setrange(k, 1, 3))
        self.assertEqual("1334", self.cli.get(k))

        self.cli.set(k, v)
        self.assertEqual(6, self.cli.setrange(k, 5, 2))
        self.assertEqual("1234\x002", self.cli.get(k))
        self.cli.flushall()

    def test_setrange_againest_wrong_type_key(self):
        k = "mykey"
        self.cli.delete(k)
        self.cli.lpush(k, "foo")
        try:
            self.cli.setrange(k, 0, "bar")
        except Exception as e:
            self.assertRegex(str(e), "WRONGTYPE")
        self.cli.flushall()

    def test_setrange_againest_out_of_range_offset(self):
        k, v = "mykey", "hello"
        self.cli.delete(k)
        try:
            self.cli.setrange(k, 512*1024*1024-4, "world")
        except Exception as e:
            self.assertRegex(str(e), "maximum allowed size")
        self.cli.set(k, v)
        try:
            self.cli.setrange(k, -1, "world")
        except Exception as e:
            self.assertRegex(str(e), "out of range")
        try:
            self.cli.setrange(k, 512*1024*1024-4, "world")
        except Exception as e:
            self.assertRegex(str(e), "maximum allowed size")
        self.cli.flushall()

    def test_getrange_againest_non_existing_key(self):
        k = "mykey"
        self.cli.delete(k)
        self.assertEqual('', self.cli.getrange(k, 0, -1))
        self.cli.flushall()

    def test_getrange_againest_string_encoded_value(self):
        k, v = "mykey", "Hello World"
        self.cli.set(k, v)
        self.assertEqual("Hell", self.cli.getrange(k, 0, 3))
        self.assertEqual("Hello World", self.cli.getrange(k, 0, -1))
        self.assertEqual("orld", self.cli.getrange(k, -4, -1))
        self.assertEqual("", self.cli.getrange(k, 5, 3))
        self.assertEqual(" World", self.cli.getrange(k, 5, 5000))
        self.assertEqual("Hello World", self.cli.getrange(k, -5000, 10000))
        self.cli.flushall()

    def test_getrange_againest_integer_encoded_value(self):
        k, v = "mykey", 1234
        self.cli.set(k, v)
        self.assertEqual("123", self.cli.getrange(k, 0, 2))
        self.assertEqual("1234", self.cli.getrange(k, 0, -1))
        self.assertEqual("234", self.cli.getrange(k, -3, -1))
        self.assertEqual("", self.cli.getrange(k, 5, 3))
        self.assertEqual("4", self.cli.getrange(k, 3, 5000))
        self.assertEqual("1234", self.cli.getrange(k, -5000, 10000))
        self.cli.flushall()

    def test_getrange_fuzzing(self):
        import random

        k = "mykey"
        for i in range(1000):
            v = self.random_string(1024)
            self.cli.set(k, v)
            start, end = random.randint(0, 1500), random.randint(0, 1500)
            self.assertEqual(v[start:end+1], self.cli.getrange(k, start, end))
        self.cli.flushall()

    def to_bit(self, s):
        ascii_list = [ord(c) for c in s]
        return ''.join([f'{a:08b}' for a in ascii_list])

    def to_str(self, b):
        byte_list = [b[i:i+8]for i in range(0, len(b), 8)]
        return ''.join([chr(int(byte, 2)) for byte in byte_list])

    def random_string(self, length):
        import random
        import string

        chars = string.ascii_letters + string.digits
        return ''.join(random.choices(chars, k=length))

    def tearDown(self):
        if self.cli is not None:
            self.cli.close()
