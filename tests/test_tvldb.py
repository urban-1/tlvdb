import os
import time
import unittest
import logging as lg

from tlvdb.tlv import TLV


class TestDB(unittest.TestCase):

    def headerDump():
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
        cls.ITEMS = 100*10
        cls.ts = TlvStorage(IFILE)
        cls.idx = ts.index


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
        t = ts.read(num)

        self.assertEqual(t.value[TLV(b"key%s" % num)], TLV(b"value%s" % num))


    def test_0002_delete(self):

        TestDB.ts.beginTransaction()
        # Delete the second half... from what we just added
        for i in range(TestDB.idx.nextid-1, int(TestDB.idx.nextid-TestDB.ITEMS/2-1), -1):
            t = TestDB.ts.delete(i, TLV)
            # print(t)
        TestDB.ts.endTransaction()

        headerDump()

    def test_0003_vacuum(self):
        TestDB.ts.vacuum()
        headerDump()
