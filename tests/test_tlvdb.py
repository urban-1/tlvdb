import os
import time
import unittest
import logging

from tlvdb.tlv import TLV
from tlvdb.tlvstorage import TlvStorage
from tlvdb.tlverrors import *

lg = logging.getLogger("tests")

class TestDB(unittest.TestCase):

    @classmethod
    def headerDump(cls):
        print(cls.idx.header.getStrInfo())
        print(cls.idx.getStrInfo())

    @classmethod
    def setUpClass(cls):
        ROOT = os.path.abspath(os.path.dirname(os.path.dirname(__file__)))
        cls.IFILE = "%s/data/test.idx" % ROOT
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
        lg.debug("")
        for i in range(TestDB.idx.nextid-1, int(TestDB.idx.nextid-TestDB.ITEMS-1), -1):
            t = TestDB.ts.read(i)
            val = ("value%d" % (i)).encode("ascii")
            key = ("key%d" % (i)).encode("ascii")
            lg.debug("testing %d: %s" % (i, t))
            self.assertEqual(t.value[TLV(key)], TLV(val))

    def test_0003_delete(self):
        lg.debug("")
        TestDB.ts.beginTransaction()
        # Delete the second half... from what we just added
        from_id = int(TestDB.idx.nextid-TestDB.ITEMS)
        to_id = int(TestDB.idx.nextid-TestDB.ITEMS/2)
        for i in range(from_id, to_id):
            t = TestDB.ts.delete(i, TLV)
            lg.debug("deleting %d: %s" % (i, t))
        TestDB.ts.endTransaction()

        TestDB.headerDump()

    def test_0004_vacuum(self):
        TestDB.ts.vacuum()
        TestDB.headerDump()

    def test_0005_read_all2(self):
        lg.debug("")
        for i in range(TestDB.idx.nextid-1, 0, -1):
            try:
                t = TestDB.ts.read(i)
            except IndexNotFoundError as e:
                lg.debug("testing %d: %s" % (i, str(e)))
                continue

            lg.debug("testing %d: %s" % (i, t))
            val = ("value%d" % (i)).encode("ascii")
            key = ("key%d" % (i)).encode("ascii")
            
            self.assertEqual(t.value[TLV(key)], TLV(val),
                msg="%s != %s" % (t.value[TLV(key)], TLV(val)))

    def test_0006_update_fitting(self):
        lg.debug("")

        tid = TestDB.idx.nextid-1
        t = TestDB.ts.read(tid)

        oldkey = TLV(("key%d" % tid).encode("ascii"))
        newvalue = TLV("-".encode("ascii"))

        t.value[oldkey] = newvalue
        TestDB.ts.update(t)

        t = TestDB.ts.read(tid)
        lg.debug("Updated TLV: %s", t)
        self.assertEqual(t.value[oldkey], newvalue)


    def test_0007_vacuum_confirm(self):
        TestDB.ts.vacuum()

        tid = TestDB.idx.nextid-1
        t = TestDB.ts.read(tid)

        oldkey = TLV(("key%d" % tid).encode("ascii"))
        newvalue = TLV("-".encode("ascii"))

        lg.debug("Updated TLV after Vacuum: %s", t)
        self.assertEqual(t.value[oldkey], newvalue)

    def test_0008_update_non_fitting(self):
        lg.debug("")

        tid = TestDB.idx.nextid-2
        t = TestDB.ts.read(tid)

        oldkey = TLV(("key%d" % tid).encode("ascii"))
        newvalue = TLV("I am not fitting for sure!".encode("ascii"))

        t.value[oldkey] = newvalue
        TestDB.ts.update(t)

        lg.debug("Updated TLV: %s", t)
        self.assertEqual(t.value[oldkey], newvalue)


    def test_0009_vacuum_confirm(self):
        lg.debug("")
        TestDB.ts.vacuum()

        tid = TestDB.idx.nextid-2
        t = TestDB.ts.read(tid)

        oldkey = TLV(("key%d" % tid).encode("ascii"))
        newvalue = TLV("I am not fitting for sure!".encode("ascii"))

        lg.debug("Updated TLV after Vacuum: %s", t)
        self.assertEqual(t.value[oldkey], newvalue)

    def test_0010_restore_changed(self):
        """
        We need this to be able to run the tests without reset
        """
        lg.debug("")

        tid1 = TestDB.idx.nextid-1
        tid2 = TestDB.idx.nextid-2
        lg.info("Putting back %d and %d" % (tid1, tid2))

        oldkey1 = TLV(("key%d" % tid1).encode("ascii"))
        newvalue1 = TLV(("value%d" % tid1).encode("ascii"))
        t = TestDB.ts.read(tid1)
        t.value[oldkey1] = newvalue1
        TestDB.ts.update(t)

        oldkey2 = TLV(("key%d" % tid2).encode("ascii"))
        newvalue2 = TLV(("value%d" % tid2).encode("ascii"))
        t = TestDB.ts.read(tid2)
        t.value[oldkey2] = newvalue2
        TestDB.ts.update(t)

        TestDB.ts.vacuum()

        t = TestDB.ts.read(tid1)
        self.assertEqual(t.value[oldkey1], newvalue1)
        t = TestDB.ts.read(tid2)
        self.assertEqual(t.value[oldkey2], newvalue2)
