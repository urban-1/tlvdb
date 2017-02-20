import os
import time
import string
import random
import logging
import unittest
from threading import Lock, Thread

from tlvdb.tlv import TLV
from tlvdb.tlvstorage import TlvStorage
from tlvdb.tlverrors import *

lg = logging.getLogger("tests")

def rand_str(N=10):
    return ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(N))

IDS=[]
lock = Lock()


def create(cls, num):
    """
    create a 2-key TLV entry
    """
    global created
    t = TLV({
                TLV(rand_str()): TLV(rand_str()),
                TLV(rand_str()): TLV(rand_str())
            })

    lg.info("%d: Creating %s" % (num, t))
    tmp = cls.ts.create(t)
    with lock:
        lg.info("%d: Added: %d" % (num, tmp))
        IDS.append(tmp)
        cls.created += 1

def delete(cls, num):
    """
    delete a random entry
    """
    global deleted
    with lock:
        try:
            tmp = random.choice(IDS)
            lg.info("Skipping update... nothing in there")
        except:
            return

    lg.info("%d: Deleting %s" % (num, tmp))
    cls.ts.delete(tmp)

    with lock:
        cls.deleted += 1

def update(cls, num):
    """
    update a random entry with another random entry
    """
    global updated
    with lock:
        try:
            tmp = random.choice(IDS)
            told = cls.ts.read(tmp)
            lg.info("Skipping update... nothing in there")
        except:
            return

    t = TLV({
                TLV(rand_str()): TLV(rand_str()),
                TLV(rand_str()): TLV(rand_str())
            })
    t._tlvdb_id = told._tlvdb_id

    lg.info("%d: Updating %d to %s" % (num, tmp, t))
    tmp = cls.ts.update(t)

    with lock:
        IDS.append(tmp)
        cls.updated += 1

class TestProc(unittest.TestCase):
    @classmethod
    def headerDump(cls):
        print(cls.idx.header.getStrInfo())
        print(cls.idx.getStrInfo())

    @classmethod
    def setUpClass(cls):
        ROOT = os.path.abspath(os.path.dirname(os.path.dirname(__file__)))
        cls.IFILE = "%s/data/proc.idx" % ROOT
        cls.MOVES = 1000
        cls.ts = TlvStorage(cls.IFILE)
        cls.idx = TestProc.ts.index
        cls.created = 0
        cls.updated = 0
        cls.deleted = 0

    def test_0001_go_wild(self):
        jobs = []
        for num in range(TestProc.MOVES):
            action = random.choice(["create", "create", "update", "delete"])
            lg.debug("Starting %s" % action)
            j = Thread(target=globals()[action], args=(TestProc, num))
            j.start()
            jobs.append(j)

        for j in jobs:
            j.join()

        # TestProc.ts.vacuum()

    def test_0002_read_all(self):
        # global created, updated, deleted
        TestProc.ts = TlvStorage(TestProc.IFILE)
        TestProc.idx = TestProc.ts.index
        lg.info("")
        TestProc.headerDump()

        lg.info("c=%d, u=%d, d=%d" % (TestProc.created, TestProc.updated, TestProc.deleted))
        for i in range(TestProc.idx.nextid-1, 0, -1):
            try:
                t = TestProc.ts.read(i)
            except IndexNotFoundError as e:
                lg.info("Delete value %d: %s" % (i, str(e)))
                continue
            lg.info("testing %d: %s" % (i, t))
