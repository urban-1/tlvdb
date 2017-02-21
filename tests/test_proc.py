import os
import time
import string
import random
import logging
import unittest
from threading import Lock, Thread
try:
    from queue import Queue, Empty
except:
    from Queue import Queue, Empty

from tlvdb.tlv import TLV
from tlvdb.tlvstorage import TlvStorage
from tlvdb.tlverrors import *

lg = logging.getLogger("tests")

def rand_str(N=None):
    if N is None:
        N = random.randint(15,20)
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

def vacuum(cls, num):
    """blocks all!"""
    with lock:
        cls.vacuum += 1
    cls.ts.vacuum()

q = Queue()
def worker_job(base):
    while True:
        try:
            args = q.get()
            globals()[args["method"]](TestProc, args["num"])
            q.task_done()
        except Empty:
            q.task_done()
        except Exception as e:
            q.task_done()
            print(str(e))


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
        cls.vacuum = 0
        cls.NUM_THREADS = 2

    def test_0001_go_wild(self):
        choices = [ "create" ] * 9
        choices.extend(["update"] * 5)
        choices.extend(["delete"] * 5)
        choices.extend(["vacuum"])

        workers = []

        for num in range(TestProc.MOVES):
            action = random.choice(choices)
            q.put({"method": action, "num": num})




        TestProc.s = time.time()
        for i in range(TestProc.NUM_THREADS):
            j = Thread(target=worker_job, args=(TestProc,))
            workers.append(j)
            j.setDaemon(True)
            j.start()

        q.join()
        TestProc.e = time.time()


    def test_0002_print_stats(self):
        TestProc.headerDump()
        totalTrans = TestProc.created + TestProc.updated + TestProc.deleted
        totalTime = (TestProc.e - TestProc.s) * 1000
        print("Total Transactions: %d" % totalTrans)
        print("        Total Time: %.2f ms" % totalTime)
        print("           Vacuums: %d" % TestProc.vacuum)
        print("           Created: %d" % TestProc.created)
        print("           Updated: %d" % TestProc.updated)
        print("           Deleted: %d" % TestProc.deleted)
        print("")
        print("Time per transaction: %.2f ms (excluding vacuum from count)" % (totalTime/totalTrans))
        print("")
        print("         # Threads: %d" % TestProc.NUM_THREADS)
        print("       Total Moves: %d" % TestProc.MOVES)

    def test_0004_read_all(self):
        # global created, updated, deleted
        TestProc.ts = TlvStorage(TestProc.IFILE)
        TestProc.idx = TestProc.ts.index
        lg.info("")

        for i in range(TestProc.idx.nextid-1, 0, -1):
            try:
                t = TestProc.ts.read(i)
            except IndexNotFoundError as e:
                lg.info("Delete value %d: %s" % (i, str(e)))
                continue
            lg.info("testing %d: %s" % (i, t))
