import logging as lg
from multiprocessing import RLock

from tlvdb.tlv import GenericPackable
from tlvdb.tlvindex import Index
from tlvdb.btree import BPlusTree
from tlvdb import util

class BPlusTreeIndex(BPlusTree):
    """
    A class to interface the B+ tree implementation (pure python gist) to the
    tlvdb architecture/interface ...
    """

    def __init__(self, filepath):
        # FIXME: A different value (!=1) here crashes the tree :'(
        # I think I fixed but not sure
        super(BPlusTreeIndex, self).__init__(20)
        self.filepath = filepath
        self.clean = True
        self.lock = RLock()
        self.load()

    def handle(self, operation, tlvid, value=None, pos=None):
        """
        Generic hook function called by tlv storage when a database operation
        takes place
        """
        lg.info("Updating index for '%s' operation" % operation)
        self.clean = False
        if operation == "create":
            # Key is the value and leaf is the ID
            self.insert(value, tlvid)
        elif operation == "delete":
            # Key is the value and leaf is the ID
            self.removeByValue(tlvid)

    def load(self):
        """
        Read the index from its file
        """
        with self.lock:
            # read the key value pairs

            fd = util.create_open(self.filepath, "rb")

            d = GenericPackable()
            d.unpack(fd)

            fd.close()

            for k,v in d.data:
                self.insert(k, v)




    def create(self, tlvid, value):
        pass

    # def get ... from parent
    # get(self, key, key2=None, startswith=None, default=None)

    def update(self, tlvid, value):
        pass

    def flush(self):
        lg.info("Flushing data index %s" % self.filepath)
        with self.lock:

            fd = util.create_open(self.filepath, "wb")
            d = GenericPackable(self.items())
            fd.write(d.pack())
            fd.flush()
            fd.close()

        self.clean = True
        print(self.items())



    def close(self):
        # make sure no one is writing/flushing
        with self.lock:
            self.fd.close()
