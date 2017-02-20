import os
import time
import string
import random
import logging
import unittest
from threading import Lock, Thread
try:
    from queue import Queue
except:
    from Queue import Queue

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

            if not tmp:
                lg.info("Skipping update... nothing in there")
                return
        except IndexError as e:
            return
        except Exception as e:
            lg.waring("WTF: %s" % str(e))

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

            if not tmp:
                lg.info("Skipping update... nothing in there")
                return
            try:
                told = cls.ts.read(tmp)
            except IndexNotFoundError:
                lg.info("Skipping node we cannot read..")
                return
        except IndexError as e:
            return
        except Exception as e:
            lg.waring("WTF: %s" % str(e))

    t = TLV({
                TLV(rand_str()): TLV(rand_str()),
                TLV(rand_str()): TLV(rand_str())
            })
    t._tlvdb_id = told._tlvdb_id

    lg.info("%d: Updating %d to %s" % (num, tmp, t))
    try:
        tmp = cls.ts.update(t)
    except IndexNotFoundError as e:
        lg.debug("Shiiit! Someone was faster: %s" % str(e))
        pass

    with lock:
        IDS.append(tmp)
        cls.updated += 1

q = Queue()
def worker_job(base):
    while True:
        args = q.get()
        globals()[args["method"]](TestProc, args["num"])
        q.task_done()


class TestProc(unittest.TestCase):
    @classmethod
    def headerDump(cls):
        print(cls.idx.header.getStrInfo())
        print(cls.idx.getStrInfo())

    @classmethod
    def setUpClass(cls):
        ROOT = os.path.abspath(os.path.dirname(os.path.dirname(__file__)))
        cls.IFILE = "%s/data/proc.idx" % ROOT
        cls.MOVES = 1000*10
        cls.ts = TlvStorage(cls.IFILE)
        cls.idx = TestProc.ts.index
        cls.created = 0
        cls.updated = 0
        cls.deleted = 0
        cls.NUM_THREADS = 1

    def test_0001_go_wild(self):
        workers = []

        for i in range(TestProc.NUM_THREADS):
            j = Thread(target=worker_job, args=(TestProc,))
            workers.append(j)
            j.setDaemon(True)
            j.start()

        for num in range(TestProc.MOVES):
            action = random.choice(["create", "create", "update", "delete"])
            q.put({"method": action, "num": num})


        q.join()


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
