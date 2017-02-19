import os
import time
import unittest
import logging as lg

import tlvdb.util as util
from tlvdb.tlv import IPackable
from tlvdb.tlverrors import *
from tlvdb.tlvstorage import TlvStorage

class Person(IPackable):

    packaged = ["name", "numbers", "age"]

    def __init__(self, name="", numbers=[], age=-1):
        self.name = name
        self.numbers = numbers
        self.age = age


class TestODB(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        ROOT = os.path.abspath(os.path.dirname(os.path.dirname(__file__)))
        cls.IFILE = "%s/data/test.idx" % ROOT
        cls.ITEMS = 1000*100
        cls.ts = TlvStorage(cls.IFILE)
        cls.idx = TestODB.ts.index


    @classmethod
    def tearDownClass(cls):
        cls.ts.close()

    def test_0001_create(self):
        p = Person("Andreas", ["0712312312", "0897408123"], 30)
        TestODB.pid = TestODB.ts.create(p)
        lg.debug("Person ID created: %d" % TestODB.pid)

    def test_0002_read(self):
        p = TestODB.ts.read(TestODB.pid, klass=Person)
        lg.debug("Got %s" % p)
