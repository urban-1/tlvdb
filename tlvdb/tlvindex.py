import struct
import logging as lg

from tlvdb.tlv import TLV, BaseIO

class IndexEntry(BaseIO):
    """
    Map a key to the position in the partition
    """
    LENGTH = 17

    def __init__(self, partition=None, key=None, offset=None, fd=None):
        super(BaseIO, self).__init__(fd)
        self.partition = partition
        self.key = key
        self.offset = offset

    def size(self):
        return IndexEntry.LENGTH

    def read(self, pos, seek=True):
        if seek:
            self.seek(pos)

        data = self.fd.read(IndexEntry.LENGTH)

        self.partition,
        self.key,
        self.offset = struct.unpack("<BQQ", data)

    def write(self, pos, seek=True):
        if seek:
            self.seek(pos)

        data = struct.pack("<BQQ",
            self.partition,
            self.key,
            self.offset
        )
        self.fd.write(data)



class IndexHeader(BaseIO):

    LENGTH = 256
    USED = 11

    TYPE_HASH = 1
    TYPE_BTREE = 2

    def __init__(self, fd):
        super(IndexHeader, self).__init__(fd)
        self.version = -1
        self.type = IndexHeader.TYPE_HASH
        self.items = 0
        self.partitions = 1

    def read(self, pos=0, seek=True):
        if seek:
            self.seek(pos)

        data = self.fd.read(IndexHeader.LENGTH)
        if not data:
            lg.warning("No data in the header!")
            return

        (self.version,
        self.type,
        self.items,
        self.partitions) = struct.unpack("<BBQB", data[:IndexHeader.USED])


    def write(self, pos=0, seek=True):
        if seek:
            self.seek(pos)

        data = struct.pack("<BBQB",
            self.version,
            self.type,
            self.items,
            self.partitions
        )

        data += b"\0" * (IndexHeader.LENGTH - IndexHeader.USED)
        self.fd.write(data)

    def size(self):
        return IndexHeader.LENGTH


class Index(object):

    def __init__(self, fd):
        self.fd = fd
        self.header = None
        self.clean = True
        self.load()

    def load(self):
        """
        Read the index from its file
        """

        # read the header
        self.header = IndexHeader(self.fd)
        self.header.read()
        if self.header.version == -1:
            lg.debug("No header in the index file... initializing")
            self._initHeader()
        self._loadIndex()

    def create(self, part, id, pos):
        pass

    def get(self, tlvid):
        pass

    def update(self, tlv):
        pass

    def delete(self, tlvid):
        pass

    def getFreePosition(self, partition, size):
        """
        Find the next available position that can fit ``size``
        """
        pass

    def flush(self):
        print("Flushing Index")
        self.header.write()
        self._dumpIndex()
        self.fd.flush()
        self.fd.truncate()

    def close(self):
        self.fd.close()


class HashIndex(Index):

    def __init__(self, *args):
        self.partitions = []
        self.nextid = 1
        super(HashIndex, self).__init__(*args)

    def reload(self):
        """
        Clean all internal variables and call load()
        """
        self.partitions = []
        self.nextid = 1
        self.load()


    def create(self, part, tid, pos):
        self.clean = False
        # Start indexing from 1: 0 is empty!
        self.partitions[part]["index"][tid] = pos + 1
        self.header.items += 1
        self.nextid += 1

    def update(self, part, tid, pos):
        self.clean = False
        # Start indexing from 1: 0 is empty!
        self.partitions[part]["index"][tid] = pos + 1

    def get(self, tlvid):
        for part, p in enumerate(self.partitions):
            if tlvid in p["index"]:
                return part, p["index"][tlvid] - 1
        return False, None


    def delete(self, tlvid):
        """
        Remove an object from the index and return its old position
        """
        for part, p in enumerate(self.partitions):
            if tlvid in p["index"]:
                # Check if already deleted...
                if p["index"][tlvid] == 0:
                    return False, None
                oldpos = p["index"][tlvid] - 1
                del p["index"][tlvid]
                self.header.items -= 1
                self.clean = False
                return part, oldpos

        return False, None

    def _initHeader(self):
        self.header.version = 1
        self.header.type = IndexHeader.TYPE_HASH
        self.header.items = 0
        self.header.partitions = 1
        self.header.write()

    def _loadIndex(self):
        # skip header (already read)
        self.fd.seek(IndexHeader.LENGTH)

        # read the whole thing
        data = self.fd.read()

        for i in range(0, self.header.partitions):
            self.partitions.append({"index":{}, "empty":0})

        # parse it
        for i in range(0, self.header.items):
            datapos = i*IndexEntry.LENGTH
            # lg.debug("Reading index entry from=%d to to=%d" % (datapos, datapos+IndexEntry.LENGTH))
            part, tid, npos = struct.unpack("<BQQ", data[datapos:datapos+IndexEntry.LENGTH])

            self.partitions[part]["index"][tid] = npos

            # if npos == 0:
            #     self.header.empty += 1

            if self.nextid <= tid:
                self.nextid = tid + 1

    def _dumpIndex(self):
        # skip header (already read)
        self.fd.seek(IndexHeader.LENGTH)

        for part, cont in enumerate(self.partitions):
            for tid, pos in cont["index"].items():
                # lg.debug("Dumping index entry port=%d pos=%d" % (part, pos))
                data = struct.pack("<BQQ", part, tid, pos)
                self.fd.write(data)

    def getStrInfo(self):
        """
        Debuging info
        """
        s = ""
        for part, cont in enumerate(self.partitions):
            s = "%sPartition: %d, Size: %d\n" % (s, part, len(cont["index"]))
        return s
