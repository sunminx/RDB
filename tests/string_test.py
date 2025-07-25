import redis
import unittest

class TestString(unittest.TestCase):
    @classmethod
    def setUpClass(self):
        self.cli = redis.Redis(host="localhost", port=6379, decode_responses=True)

    def setUp(self):
        self.k = "foo"
        self.v = "bar"

    def test_setget(self):
        self.cli.set(self.k, self.v)
        self.assertEqual(self.v, self.cli.get(self.k))

    def test_setgetempty(self):
        self.cli.set(self.k, self.v)
        self.assertEqual(self.v, self.cli.get(self.k))

    def test_setdel(self):
        self.cli.set(self.k, self.v)
        self.assertEqual(self.v, self.cli.get(self.k))
        self.cli.delete(self.k)
        self.assertNotEqual(self.v, self.cli.get(self.k))

    def test_bigpayload(self):
        self.v *= 1000000
        self.cli.set(self.k, self.v)
        self.assertEqual(self.v, self.cli.get(self.k))

    def test_randomaccess(self):
        import random

        payload = {}
        for i in range(1000):
            size = random.randint(1, 10000)
            self.k, self.v = f"bigpayload{i}", f"pl-{i}"*size
            payload[self.k] = self.v
            self.cli.set(self.k, self.v)
        for self.key, self.val in payload.items():
            self.assertEqual(self.v, self.cli.get(self.k))

    def test_numself_key(self):
        for i in range(10000):
            self.cli.set(i, i)
        for i in range(9999, -1, -1):
            self.assertEqual(str(i), self.cli.get(i))
        # checself.k db size before flush
        self.assertEqual(10000, self.cli.dbsize())

    def test_setnx_keymissing(self):
        self.cli.delete(self.k)
        self.assertEqual(1, self.cli.setnx(self.k, self.v))
        self.assertEqual(self.v, self.cli.get(self.k))

    def test_setnx_keyexists(self):
        self.cli.set(self.k, self.v)
        self.assertEqual(0, self.cli.setnx(self.k, self.v))
        self.assertEqual(self.v, self.cli.get(self.k))

    def test_setnx_not_expired_key(self):
        self.cli.set(self.k, self.v)
        self.cli.expire(self.k, 10000)
        self.assertEqual(0, self.cli.setnx(self.k, 20))
        self.assertEqual(self.v, self.cli.get(self.k))

    def test_setnx_againest_expiried_volatile_key(self):
        import time

        for i in range(10000):
            self.cli.setex(f'self.key-{i}', 3600, "self.value")
        self.cli.set("self.key", 10)
        self.cli.expire("self.key", 1)
        time.sleep(2)
        self.assertEqual(1, self.cli.setnx("self.key", 20))
        self.assertEqual("20", self.cli.get("self.key"))

    def test_mget(self):
        k1, k2 = "foo", "bar"
        v1, v2 = "BAR", "FOO"
        self.cli.set(k1, v1)
        self.cli.set(k2, v2)
        self.assertEqual([v1, v2], self.cli.mget([k1, k2]))

    def test_mget_not_exists_key(self):
        k1, k2 = "foo", "bar"
        v1, v2 = "BAR", "FOO"
        self.cli.set(k1, v1)
        self.cli.set(k2, v2)
        self.assertEqual([v1, None, v2], self.cli.mget([k1, "self.key", k2]))

    def test_append(self):
        self.cli.set(self.k, self.v)
        self.assertEqual(self.v, self.cli.get(self.k))
        self.v += "bar"
        self.cli.set(self.k, self.v)
        self.assertEqual(self.v, self.cli.get(self.k))

    def test_mset(self):
        k1, k2, k3 = "x", "y", "z"
        v1, v2, v3 = "10", "foobar", "x x x x x x x\\n\\n\\r\\n"
        self.cli.mset({k1: v1, k2: v2, k3: v3})
        self.assertEqual([v1, v2, v3], self.cli.mget([k1, k2, k3]))

    def test_mset_wrong_number_keys(self):
        k1, k2, k3 = "x", "y", "z"
        v1, v2 = "10", "foobar"
        try:
            self.cli.execute_command("MSET", k1, v1, k2, v2, k3)
        except Exception as e:
            self.assertRegex(str(e), r"wrong number")

    def test_msetnx_exists_key(self):
        k1, k2, k3 = "x", "y", "z"
        v1, v2, v3 = "10", "foobar", "x x x x x x x\\n\\n\\r\\n"
        self.cli.set(k2, v2)
        self.assertEqual(0, self.cli.msetnx({k1:v1, k2:v2, k3:v3}))
        self.assertEqual(None, self.cli.get(k1))
        self.assertEqual(None, self.cli.get(k3))

    def test_msetnx_not_exists_key(self):
        k1, k2 = "x1", "x2"
        v1, v2 = "xxx", "yyy"
        self.assertEqual(1, self.cli.msetnx({k1:v1, k2:v2}))
        self.assertEqual(v1, self.cli.get(k1))
        self.assertEqual(v2, self.cli.get(k2))

    def test_non_exists_key(self):
        self.assertEqual(0, self.cli.strlen("notaself.key"))

    def test_int_encoded_value(self):
        k, v = "myint", -555
        self.cli.set(k, v)
        self.assertEqual(4, self.cli.strlen(k))

    def test_plain_str_value(self):
        self.k, self.v = "mystr", "foozzz0123456789 baz"
        self.cli.set(self.k, self.v)
        self.assertEqual(20, self.cli.strlen(self.k))

    def test_setbit_non_existing_key(self):
        self.cli.delete(self.k)
        self.assertEqual(0, self.cli.setbit(self.k, 2, 1))
        self.assertEqual(1, self.cli.getbit(self.k, 2))
        self.assertEqual('00100000', self.to_bit(self.cli.get(self.k)))

    def test_setbit_againest_string_encoded_key(self):
        # @ 64 01000000
        self.k, self.v = "myself.key", "@"
        self.cli.set(self.k, self.v)
        self.assertEqual(0, self.cli.setbit(self.k, 2, 1))
        self.assertEqual('01100000', self.to_bit(self.cli.get(self.k)))
        self.assertEqual(1, self.cli.setbit(self.k, 1, 0))
        self.assertEqual('00100000', self.to_bit(self.cli.get(self.k)))

    def test_setbit_againest_integer_encoded_key(self):
        # 1 49 00110001
        self.k, self.v = "myself.key", 1
        self.cli.set(self.k, self.v)
        self.assertEqual(0, self.cli.setbit(self.k, 6, 1))
        self.assertEqual('00110011', self.to_bit(self.cli.get(self.k)))
        self.assertEqual(1, self.cli.setbit(self.k, 2, 0))
        self.assertEqual('00010011', self.to_bit(self.cli.get(self.k)))

    def test_setbit_againest_wrong_type_key(self):
        self.cli.delete(self.k)
        self.cli.lpush(self.k, self.v)
        try:
            self.cli.setbit(self.k, 0, 1)
        except Exception as e:
            self.assertRegex(str(e), "WRONGTYPE")

    def test_setbit_with_out_of_range_offset(self):
        try:
            self.cli.setbit(self.k, 4*1024*1024*1024, 1)
        except Exception as e:
            self.assertRegex(str(r), "out of range")

    def test_setbit_with_out_of_range_offset(self):
        bs = [-1, 2, 10, 20]
        for b in bs:
            try:
                self.cli.setbit(self.k, 0, b)
            except Exception as e:
                self.assertRegex(str(r), "out of range")

    def test_setbit_fuzzing(self):
        import random

        size = 256
        ln = size*8
        k = "myself.key"
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

    def test_getbit_againest_non_existing_key(self):
        self.cli.delete(self.k)
        self.assertEqual(0, self.cli.getbit(self.k, 0))

    def test_getbit_againest_string_encoded_key(self):
        # ` 96 01100000
        self.k, self.v = "myself.key", "`"
        self.cli.set(self.k, self.v)
        self.assertEqual(0, self.cli.getbit(self.k, 0))
        self.assertEqual(1, self.cli.getbit(self.k, 1))
        self.assertEqual(1, self.cli.getbit(self.k, 2))
        self.assertEqual(0, self.cli.getbit(self.k, 3))
        self.assertEqual(0, self.cli.getbit(self.k, 8))
        self.assertEqual(0, self.cli.getbit(self.k, 100))
        self.assertEqual(0, self.cli.getbit(self.k, 1000))

    def test_getbit_againest_integer_encoded_key(self):
        # 1 49 00110000
        self.k, self.v = "myself.key", 1
        self.cli.set(self.k, self.v)
        self.assertEqual(0, self.cli.getbit(self.k, 0))
        self.assertEqual(0, self.cli.getbit(self.k, 1))
        self.assertEqual(1, self.cli.getbit(self.k, 2))
        self.assertEqual(1, self.cli.getbit(self.k, 3))
        self.assertEqual(0, self.cli.getbit(self.k, 8))
        self.assertEqual(0, self.cli.getbit(self.k, 100))
        self.assertEqual(0, self.cli.getbit(self.k, 1000))

    def test_setrange_againest_non_existing_key(self):
        self.cli.delete(self.k)
        self.assertEqual(3, self.cli.setrange(self.k, 0, "foo"))
        self.assertEqual("foo", self.cli.get(self.k))
        self.cli.delete(self.k)
        self.assertEqual(0, self.cli.setrange(self.k, 0, ""))
        self.assertEqual(None, self.cli.get(self.k))
        self.cli.delete(self.k)
        self.assertEqual(4, self.cli.setrange(self.k, 1, "foo"))
        self.assertEqual("\0foo", self.cli.get(self.k))

    def test_setrange_againest_string_encoded_value(self):
        self.k, self.v = "myself.key", "foo"
        self.cli.set(self.k, self.v)
        self.assertEqual(3, self.cli.setrange(self.k, 0, "b"))
        self.assertEqual("boo", self.cli.get(self.k))

        self.cli.set(self.k, self.v)
        self.assertEqual(3, self.cli.setrange(self.k, 0, ""))
        self.assertEqual("foo", self.cli.get(self.k))

        self.cli.set(self.k, self.v)
        self.assertEqual(3, self.cli.setrange(self.k, 1, "b"))
        self.assertEqual("fbo", self.cli.get(self.k))

        self.cli.set(self.k, self.v)
        self.assertEqual(7, self.cli.setrange(self.k, 4, "bar"))
        self.assertEqual("foo\0bar", self.cli.get(self.k))

    def test_setrange_againest_integer_encoded_value(self):
        self.k, self.v = "myself.key", 1234
        self.cli.set(self.k, self.v)
        self.assertEqual(4, self.cli.setrange(self.k, 0, 2))
        self.assertEqual("2234", self.cli.get(self.k))

        self.cli.set(self.k, self.v)
        self.assertEqual(4, self.cli.setrange(self.k, 0, ""))
        self.assertEqual("1234", self.cli.get(self.k))

        self.cli.set(self.k, self.v)
        self.assertEqual(4, self.cli.setrange(self.k, 1, 3))
        self.assertEqual("1334", self.cli.get(self.k))

        self.cli.set(self.k, self.v)
        self.assertEqual(6, self.cli.setrange(self.k, 5, 2))
        self.assertEqual("1234\x002", self.cli.get(self.k))

    def test_setrange_againest_wrong_type_key(self):
        self.k = "myself.key"
        self.cli.delete(self.k)
        self.cli.lpush(self.k, "foo")
        try:
            self.cli.setrange(self.k, 0, "bar")
        except Exception as e:
            self.assertRegex(str(e), "WRONGTYPE")

    def test_setrange_againest_out_of_range_offset(self):
        self.k, self.v = "myself.key", "hello"
        self.cli.delete(self.k)
        try:
            self.cli.setrange(self.k, 512*1024*1024-4, "world")
        except Exception as e:
            self.assertRegex(str(e), "maximum allowed size")
        self.cli.set(self.k, self.v)
        try:
            self.cli.setrange(self.k, -1, "world")
        except Exception as e:
            self.assertRegex(str(e), "out of range")
        try:
            self.cli.setrange(self.k, 512*1024*1024-4, "world")
        except Exception as e:
            self.assertRegex(str(e), "maximum allowed size")

    def test_getrange_againest_non_existing_key(self):
        self.cli.delete(self.k)
        self.assertEqual('', self.cli.getrange(self.k, 0, -1))

    def test_getrange_againest_string_encoded_value(self):
        self.k, self.v = "myself.key", "Hello World"
        self.cli.set(self.k, self.v)
        self.assertEqual("Hell", self.cli.getrange(self.k, 0, 3))
        self.assertEqual("Hello World", self.cli.getrange(self.k, 0, -1))
        self.assertEqual("orld", self.cli.getrange(self.k, -4, -1))
        self.assertEqual("", self.cli.getrange(self.k, 5, 3))
        self.assertEqual(" World", self.cli.getrange(self.k, 5, 5000))
        self.assertEqual("Hello World", self.cli.getrange(self.k, -5000, 10000))

    def test_getrange_againest_integer_encoded_value(self):
        self.k, self.v = "myself.key", 1234
        self.cli.set(self.k, self.v)
        self.assertEqual("123", self.cli.getrange(self.k, 0, 2))
        self.assertEqual("1234", self.cli.getrange(self.k, 0, -1))
        self.assertEqual("234", self.cli.getrange(self.k, -3, -1))
        self.assertEqual("", self.cli.getrange(self.k, 5, 3))
        self.assertEqual("4", self.cli.getrange(self.k, 3, 5000))
        self.assertEqual("1234", self.cli.getrange(self.k, -5000, 10000))

    def test_getrange_fuzzing(self):
        import random

        self.k = "myself.key"
        for i in range(1000):
            self.v = self.random_string(1024)
            self.cli.set(self.k, self.v)
            start, end = random.randint(0, 1500), random.randint(0, 1500)
            self.assertEqual(self.v[start:end+1], self.cli.getrange(self.k, start, end))

    def test_getrange_huge_range(self):
        self.cli.set(self.k, self.v)
        self.assertEqual(self.v, self.cli.getrange(self.k, 0, 4294967297))

    def test_set_detect_syntax_err(self):
        try:
            self.cli.execute_command("SET", self.k, self.v, "non-existing-option")
        except Exception as e:
            self.assertRegex(str(e), r"syntax")

    def test_setnx(self):
        self.cli.delete(self.k)
        self.assertEqual(1, self.cli.setnx(self.k, 1))
        self.assertEqual(False, self.cli.setnx(self.k, 2))
        self.assertEqual("1", self.cli.get(self.k))

    def test_setxx(self):
        self.cli.delete(self.k)
        self.assertEqual(None, self.cli.set(self.k, 1, xx=True))
        self.cli.set(self.k, self.v)
        self.assertEqual(1, self.cli.set(self.k, 2, xx=True))
        self.assertEqual("2", self.cli.get(self.k))

    def test_setex(self):
        import time

        self.cli.delete(self.k)
        self.cli.setex(self.k, 3, self.v)
        time.sleep(3)
        self.assertEqual(None, self.cli.get(self.k))

    def test_setpx(self):
        import time

        self.cli.delete(self.k)
        self.cli.set(self.k, self.v, px=3000)
        time.sleep(3)
        self.assertEqual(None, self.cli.get(self.k))

    def test_set_multi_options(self):
        self.cli.delete(self.k)
        self.assertEqual(None, self.cli.set(self.k, self.v, xx=True, px=3000))
        self.assertEqual(None, self.cli.get(self.k))

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
            self.cli.flushall()
        self.k = None
        self.v = None

    @classmethod
    def tearDownClass(self):
        if self.cli is not None:
            self.cli.close()
