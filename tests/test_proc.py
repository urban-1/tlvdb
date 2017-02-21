import os
import time
import string
import random
import logging
import unittest
from threading import Lock, Thread, currentThread
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


def create(cls, tag):
    """
    create a 2-key TLV entry
    """
    global created
    t = TLV({
                TLV(rand_str()): TLV(rand_str()),
                TLV(rand_str()): TLV(rand_str())
            })

    lg.info("%s: Creating %s" % (tag, t))
    tmp = cls.ts.create(t)
    lg.info("%s: Creating: index done" % tag)

    with lock:
        lg.info("%s: Added: %d" % (tag, tmp))
        IDS.append(tmp)
        cls.created += 1

def delete(cls, tag):
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

    lg.info("%s: Deleting %s" % (tag, tmp))
    cls.ts.delete(tmp)
    lg.info("%s: Deleting: index done" % tag)

    with lock:
        cls.deleted += 1

def update(cls, tag):
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
                lg.info("%s: Reading %d" % (tag, tmp))
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

    lg.info("%s: Updating %d to %s" % (tag, tmp, t))
    try:
        tmp = cls.ts.update(t)
    except IndexNotFoundError as e:
        lg.debug("Shiiit! Someone was faster: %s" % str(e))
        return

    with lock:
        IDS.append(tmp)
        cls.updated += 1

def vacuum(cls, tag):
    """blocks all!"""
    with lock:
        cls.vacuum += 1
    cls.ts.vacuum()

q = Queue()
def worker_job(base):
    global q
    name = currentThread().getName()
    lg.info("Thread %s started" % name)
    while True:
        try:
            lg.info("Thread %s reading" % name)
            args = q.get()
            # These threads dont seem to want to exit peacefully...
            lg.info("Thread %s-%s got %s" % (name, args["num"], args["method"]))
            globals()[args["method"]](TestProc, "%s-%s" % (name, args["num"]))
            q.task_done()
        except Empty:
            break
        except Exception as e:
            q.task_done()
            pass

    lg.info("Thread %s done" % name)


class TestProc(unittest.TestCase):
    @classmethod
    def headerDump(cls):
        print(cls.idx.header.getStrInfo())
        print(cls.idx.getStrInfo())

    @classmethod
    def setUpClass(cls):
        ROOT = os.path.abspath(os.path.dirname(os.path.dirname(__file__)))
        cls.IFILE = "%s/data/proc.idx" % ROOT
        cls.MOVES = 10000
        cls.ts = TlvStorage(cls.IFILE)
        cls.idx = TestProc.ts.index
        cls.created = 0
        cls.updated = 0
        cls.deleted = 0
        cls.vacuum = 0
        cls.NUM_THREADS = 5

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
            j.daemon = True
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
                lg.info("Delete value %s: %s" % (i, str(e)))
                continue
            lg.info("testing %s: %s" % (i, t))
