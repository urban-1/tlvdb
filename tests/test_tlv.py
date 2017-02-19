import os
import time
import unittest
import logging as lg

import tlvdb.util as util
from tlvdb.tlv import TLV


class TestTLV(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        ROOT = os.path.abspath(os.path.dirname(os.path.dirname(__file__)))
        cls.FILE = "%s/data/tlv.dat" % ROOT
        cls.FD = util.create_open(cls.FILE)

    @classmethod
    def tearDownClass(cls):
        cls.FD.close()

    def test_0001_create(self):
        TestTLV.t = TLV([
            TLV(16),
            TLV(32),
            TLV(b"ab"),
            TLV({TLV(b"key"): TLV(b"value")}),
            TLV([
                TLV(2560),
                TLV(25600),
                TLV(256000),
                TLV(2560000),
                TLV(25600000)
                ]),
            TLV(0.00000001),
            ])

        self.assertEqual(TestTLV.t.value[0].value, 16)
        self.assertEqual(TestTLV.t.value[1].value, 32)
        self.assertEqual(TestTLV.t.value[2].value, b"ab")
        self.assertEqual(TestTLV.t.value[3].value[TLV(b"key")], TLV(b"value"))


    def test_0002_write(self):
        TestTLV.FD.seek(0)
        TestTLV.FD.truncate()

        # Set stream
        TestTLV.t.setSource(TestTLV.FD)

        # Dump
        s = time.time()
        TestTLV.t.write(0)
        TestTLV.FD.flush()
        e = time.time()
        # lg.info("WRITE TIME: %f ms" % ((e-s)*1000))

    def test_0003_read(self):
        s = time.time()
        t2 = TLV(fd=TestTLV.FD)
        t2.read(0)
        e = time.time()
        # lg.info("READ TIME: %f ms" % ((e-s)*1000))

        self.assertEqual(TestTLV.t.value[0].value, 16)
        self.assertEqual(TestTLV.t.value[1].value, 32)
        self.assertEqual(TestTLV.t.value[2].value, b"ab")
        self.assertEqual(TestTLV.t.value[3].value[TLV(b"key")], TLV(b"value"))
