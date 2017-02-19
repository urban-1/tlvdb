import os
import logging as lg

import tlvdb.util as util
from tlvdb.tlv import TLV
from tlvdb.tlvindex import HashIndex
from tlvdb.tlverrors import *

IO_BUFFER_LEN = 1000000 * 1
# IO_BUFFER_LEN = 10


class TlvStorage(object):

    VERSION = 1
    """Storage version"""

    def __init__(self, index_file, vacuum_thres=1, always_append=True):
        """
        :param str index_file: Path to the main index file
        """
        # Sort out files
        self.basename = os.path.basename(index_file)
        self.dirname = os.path.abspath(os.path.dirname(index_file))
        self.basename = self.basename[0:self.basename.index(".")]
        self.in_trance = False
        self._writeBuffer = {"len": 0, "buf": bytes()}

        # open fds
        self.ifd = util.create_open(index_file)
        self.index = HashIndex(self.ifd)

        # data file descriptors
        self.dfds = []
        for p in range(0, self.getHeader().partitions):
            tmppath = "%s/%s.%d.dat" % (self.dirname, self.basename, p)
            # Open partition
            tmpfd = util.create_open(tmppath, "r+b", buffering=IO_BUFFER_LEN)
            self.dfds.append({"fd": tmpfd, "path": tmppath})

        self.clean = True

    def _flushWriteBuffer(self):

        if self._writeBuffer["len"] > 0:
            lg.info("Flusing Storage write buffer")
            part = self._writeBuffer["part"]
            self.dfds[part]["fd"].seek(self._writeBuffer["pos"])
            self.dfds[part]["fd"].write(self._writeBuffer["buf"])
            self.dfds[part]["fd"].flush()
            self.dfds[part]["last"] += self._writeBuffer["len"]
            self._writeBuffer["pos"] += self._writeBuffer["len"]
            self._writeBuffer["len"] = 0
            self._writeBuffer["buf"] = b""

    def _getDataFileEnd(self, part):
        try:
            return self.dfds[part]["last"]
        except:
            pass

        self.dfds[part]["fd"].seek(0,2)
        self.dfds[part]["last"] = self.dfds[part]["fd"].tell()
        return self.dfds[part]["last"]

    def beginTransaction(self):
        self.in_trance = True

    def endTransaction(self):
        self.in_trance = False
        self.index.flush()
        self._flushWriteBuffer()

        for p in self.dfds:
            p["fd"].flush()

    def create(self, packable):
        """
        Create a new entry in the database from the given tlv
        """
        # 1. Find next available ID
        nextid = self.index.nextid

        # 2. Find the best partition (TODO)
        part = 0

        # 3. Find next pos in data file (TODO: consult index)
        pos = self._getDataFileEnd(part)

        # 4. Write data
        data = packable.pack()
        datalen = len(data)

        # If in transaction, use the buffer
        if self.in_trance is True:
            self._writeBuffer["pos"] = pos
            # update position for the index
            pos = self._writeBuffer["len"] + pos
            self._writeBuffer["part"] = part
            self._writeBuffer["buf"] += data
            self._writeBuffer["len"] += datalen

            if self._writeBuffer["len"] >= IO_BUFFER_LEN:
                self._flushWriteBuffer()
        else:

            self.dfds[part]["fd"].seek(pos)
            self.dfds[part]["fd"].write(data)
            self.dfds[part]["last"] += datalen

        # 5. Update index
        self.index.create(part, nextid, pos)
        if self.in_trance is False:
            self.index.flush()
            self.dfds[part]["fd"].flush()

    def read(self, tid, klass=TLV, criteria=None):
        """
        Read an object with the given ID into the given class. Default class is
        TLV. The class should implement IPackable and have a default constructor
        """
        instance = klass()
        part, pos = self.index.get(tid)
        if part is False:
            raise IndexNotFoundError("Could not find item with id=%d" % tid)

        self.dfds[part]["fd"].seek(pos)
        return instance.unpack(self.dfds[part]["fd"])

    def delete(self, tid, klass=None):
        """
        Delete an entry. If class is given, the deleted entry will be returned
        """
        part, oldpos = self.index.delete(tid)

        if part is False:
            return False

        # we have deleted, check if we should return the old entry...
        ret = True
        if klass:
            instance = klass()
            self.dfds[part]["fd"].seek(oldpos)
            ret = instance.unpack(self.dfds[part]["fd"])

        # In any case, flush index
        if self.in_trance is False:
            self.index.flush()

        return ret

    def close(self):
        self.index.close()
        for fd in self.dfds:
            fd["fd"].close()

    def vacuum(self):
        """
        Compact the free space in partitions:

        - Create a new temp partition
        - Init our buffered writter
        - For every item in the main index
            - write it in the new partition
            - update the index
        - Flush/reset the buffered writer
        - Swap the temp partition with the real one
        - Flush the index
        """
        if self.in_trance:
            raise AlreadyInTrance("In the middle of transaction, vacuum was called!")
        # Create swap
        swap_part = len(self.dfds)
        swap_path = "%s/%s.%d.dat" % (self.dirname, self.basename, swap_part)
        self.dfds.append({})
        self.dfds[swap_part]["last"] = 0
        self.dfds[swap_part]["fd"] = open(swap_path, "wb")

        # Init buffered write
        new_pos = 0
        self._writeBuffer["pos"] = new_pos
        self._writeBuffer["part"] = swap_part
        self._writeBuffer["buf"] = b""
        self._writeBuffer["len"] = 0

        # Iterate, read, write
        for part, cont in enumerate(self.index.partitions):
            lg.info("Vacuum: partition %d" % part)

            for tid, pos in cont["index"].items():
                if pos == 0:
                    lg.warning("Skipping empty/deleted index?")
                    continue

                tmptlv = TLV(fd=self.dfds[part]["fd"])
                # REMEMBER: pos==0 means empty!
                lg.debug("Reading from pos=%d" % (pos-1))
                data_len = tmptlv.read(pos-1)
                lg.debug("Got data length=%d: %s" % (data_len, tmptlv))

                self._writeBuffer["len"] += data_len
                self._writeBuffer["buf"] += tmptlv.pack()

                # In memory update of the index
                lg.debug("Updating index with %d=>%d (with +1 offset)" % (tid, new_pos + 1))
                cont["index"][tid] = new_pos + 1

                new_pos += data_len

                if self._writeBuffer["len"] >= IO_BUFFER_LEN:
                    self._flushWriteBuffer()

            # Flash whatever remainder
            self._flushWriteBuffer()

            # Clean up temp partition
            self.dfds[swap_part]["fd"].flush()
            self.dfds[swap_part]["fd"].close()
            del self.dfds[swap_part]

            # Clean up real partition
            self.dfds[part]["fd"].close()

            # DANGEROUS PART: SHOULD NOT BE INTERUPTED
            orig_part = "%s/%s.%d.dat" % (self.dirname, self.basename, part)
            try:
                os.rename(swap_path, orig_part)
            except:
                lg.critical("Failed to move packed parition...")
                self.index.reload()
            else:
                self.index.flush()
                # Reopen real partition
                self.dfds[part]["fd"] = util.create_open(orig_part)




    def getHeader(self):
        return self.index.header
