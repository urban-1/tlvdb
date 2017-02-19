import os
import time
import unittest
import logging as lg

from tlvdb.tlv import TLV
from tlvdb.tlvdb import TlvStorage
from tlvdb.tlverrors import *


class TestDB(unittest.TestCase):

    @classmethod
    def headerDump(cls):
        print("%15s: %s" % ("Header Version", cls.idx.header.version))
        print("%15s: %s" % ("Index Type", cls.idx.header.type))
        print("%15s: %s" % ("Num Entries", cls.idx.header.items))
        # print("%15s: %s" % ("Num Empty", idx.header.empty))
        print("%15s: %s" % ("Partitions", cls.idx.header.partitions))

    @classmethod
    def setUpClass(cls):
        ROOT = os.path.abspath(os.path.dirname(os.path.dirname(__file__)))
        cls.IFILE = "%s/data/test.idx" % ROOT
        # ITEMS = 100*100
        # cls.ITEMS = 100*10
        cls.ITEMS = 10
        cls.ts = TlvStorage(cls.IFILE)
        cls.idx = TestDB.ts.index


    @classmethod
    def tearDownClass(cls):
        cls.ts.close()

    def test_0001_create(self):

        TestDB.ts.beginTransaction()

        for i in range(0, TestDB.ITEMS):
            t = TLV({
                        TLV(("key%d" % (TestDB.idx.nextid)).encode("ascii")): TLV(("value%d" % (TestDB.idx.nextid)).encode("ascii"))
                    })
            TestDB.ts.create(t)

        TestDB.ts.endTransaction()

        num = TestDB.idx.nextid-1
        t = TestDB.ts.read(num)

        TestDB.headerDump()
        val = ("value%d" % (num)).encode("ascii")
        key = ("key%d" % (num)).encode("ascii")
        self.assertEqual(t.value[TLV(key)], TLV(val))

    def test_0002_read_all(self):
        for i in range(TestDB.idx.nextid-1, int(TestDB.idx.nextid-TestDB.ITEMS-1), -1):
            t = TestDB.ts.read(i)
            val = ("value%d" % (i)).encode("ascii")
            key = ("key%d" % (i)).encode("ascii")
            # lg.debug("testing %d: %s" % (i, t))
            self.assertEqual(t.value[TLV(key)], TLV(val))

    def test_0003_delete(self):

        TestDB.ts.beginTransaction()
        # Delete the second half... from what we just added
        from_id = int(TestDB.idx.nextid-TestDB.ITEMS)
        to_id = int(TestDB.idx.nextid-TestDB.ITEMS/2)
        for i in range(from_id, to_id):
            t = TestDB.ts.delete(i, TLV)
            # print(t)
        TestDB.ts.endTransaction()

        TestDB.headerDump()

    def test_0004_vacuum(self):
        print(TestDB.idx.getStrInfo())
        TestDB.ts.vacuum()
        TestDB.headerDump()
        print(TestDB.idx.getStrInfo())

    def test_0005_read_all2(self):
        for i in range(TestDB.idx.nextid, 0, -1):
            try:
                t = TestDB.ts.read(i)
            except IndexNotFoundError as e:
                lg.debug("testing %d: %s" % (i, str(e)))
                continue

            val = ("value%d" % (i)).encode("ascii")
            key = ("key%d" % (i)).encode("ascii")
            lg.debug("testing %d: %s" % (i, t))
            self.assertEqual(t.value[TLV(key)], TLV(val))
