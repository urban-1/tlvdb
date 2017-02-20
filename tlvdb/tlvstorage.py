import os
import time
import signal
import logging as lg

from tlvdb import util
from tlvdb.tlv import TLV
from tlvdb.tlvindex import HashIndex
from tlvdb.tlverrors import *
from tlvdb.util import DelayedInterrupt

# x MB buffer
IO_BUFFER_LEN = 1000000


class TlvStorage(object):

    VERSION = 1
    """Storage version"""

    def __init__(self, index_file, backfill=False):
        """
        :param str index_file: Path to the main index file
        """
        self.backfill = backfill

        # Sort out files
        self.basename = os.path.basename(index_file)
        self.dirname = os.path.abspath(os.path.dirname(index_file))
        self.basename = self.basename[0:self.basename.index(".")]
        self.in_trance = False

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

    def _getDataFileEnd(self, part):
        try:
            return self.dfds[part]["last"]
        except:
            pass

        self.dfds[part]["fd"].seek(0,2)
        self.dfds[part]["last"] = self.dfds[part]["fd"].tell()
        return self.dfds[part]["last"]

    def _findAGoodPossiotion(self, part, size):
        return self._getDataFileEnd(part)


    def beginTransaction(self):
        self.in_trance = True

    def endTransaction(self):
        self.in_trance = False
        self.index.flush()

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
        data = packable.pack()
        datalen = len(data)
        pos = self._findAGoodPossiotion(part, datalen)

        # 4. Write data
        # If in transaction, use the buffer
        self.dfds[part]["fd"].seek(pos)
        self.dfds[part]["fd"].write(data)
        self.dfds[part]["last"] += datalen

        # 5. Update index
        self.index.create(part, nextid, pos)
        if self.in_trance is False:
            self.index.flush()
            self.dfds[part]["fd"].flush()

        return nextid

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
        instance.unpack(self.dfds[part]["fd"])
        instance._tlvdb_id = tid
        instance._tlvdb_clean = True
        return instance

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

        # Handle index
        self._handleEmptying(part, oldpos)

        # In any case, flush index
        if self.in_trance is False:
            self.index.flush()

        return ret

    def update(self, obj):
        if not hasattr(obj, "_tlvdb_id"):
            raise WrongInstanceError()

        # Read original object
        old = self.read(obj._tlvdb_id)

        # get old index
        part, oldpos = self.index.get(obj._tlvdb_id)

        new_data = obj.pack()
        old_data = old.pack()

        datalen = len(new_data)

        # See if we can fit it!
        if datalen <= len(old_data):
            lg.debug("Update: Object is fitting in its old place")
            pos = oldpos
        else:
            lg.debug("Update: Object is NOT fitting")
            pos = self._findAGoodPossiotion(part, datalen)
            self._handleEmptying(part, oldpos)

            # If we append, remember the partitions last byte
            if self.dfds[part]["last"] == pos:
                self.dfds[part]["last"] += datalen


        # No transaction support since we are not writing in a continues blocks
        self.dfds[part]["fd"].seek(pos)
        self.dfds[part]["fd"].write(new_data)

        # Update the index
        self.index.update(part, obj._tlvdb_id, pos)
        self.index.flush()
        self.dfds[part]["fd"].flush()

    def _handleEmptying(self, part, oldpos):
        """
        Called when something is moved or deleted
        """
        if self.backfill is True:
            # Log with detail
            tmp_instance = TLV(fd=self.dfds[part]["fd"])
            del_size = tmp_instance.size(oldpos)
            self.index.setEmpty(part, oldpos, del_size)
        else:
            # Just log to indicate dirty partition
            self.index.setEmpty(part, oldpos, 0)


    def close(self):
        self.index.close()
        for fd in self.dfds:
            fd["fd"].close()

    def vacuum(self, force=False):
        """
        Compact the free space in partitions:

        - Create a new temp partition
        - For every item in the main index
            - write it in the new partition
            - update the index
        - Flush/reset the buffered writer
        - Swap the temp partition with the real one
        - Flush the index
        """
        if self.in_trance:
            raise AlreadyInTranceError("In the middle of transaction, vacuum was called!")
        # Create swap
        swap_part = len(self.dfds)
        swap_path = "%s/%s.%d.dat" % (self.dirname, self.basename, swap_part)
        self.dfds.append({})
        self.dfds[swap_part]["last"] = 0
        self.dfds[swap_part]["fd"] = open(swap_path, "wb")

        # Start at the beginning
        new_pos = 0

        # Iterate, read, write
        for part, cont in enumerate(self.index.partitions):
            empty = len(cont["empty"])
            items = cont["items"]
            lg.info("Vacuum: partition %d, status %d/%d" % (part, empty, items))
            if (empty == 0 and not force):
                lg.info("Skipping ... partition is clean")
                continue
            lg.info(" ... Vacuum: Starting ")

            for tid, pos in cont["index"].items():
                if pos == 0:
                    lg.warning("Skipping empty/deleted index?")
                    continue

                tmptlv = TLV(fd=self.dfds[part]["fd"])
                # REMEMBER: pos==0 means empty!
                lg.debug("Reading from pos=%d" % (pos-1))
                data_len = tmptlv.read(pos-1)
                lg.debug("Got data length=%d: %s" % (data_len, tmptlv))

                self.dfds[swap_part]["fd"].write(tmptlv.pack())

                # In memory update of the index
                lg.debug("Updating index with %d=>%d (with +1 offset)" % (tid, new_pos + 1))
                cont["index"][tid] = new_pos + 1

                new_pos += data_len

            # Flash whatever remainder
            self.dfds[part]["fd"].flush()

            # Clean up temp partition
            self.dfds[swap_part]["fd"].flush()
            self.dfds[swap_part]["fd"].close()
            del self.dfds[swap_part]

            # Clean up real partition
            self.dfds[part]["fd"].close()
            del self.dfds[part]["last"]

            # DANGEROUS PART: SHOULD NOT BE INTERUPTED
            orig_part = "%s/%s.%d.dat" % (self.dirname, self.basename, part)
            with DelayedInterrupt(signal.SIGINT):
                try:
                    os.rename(swap_path, orig_part)
                except:
                    lg.critical("Failed to move packed parition...")
                    self.index.reload()
                else:
                    cont["empty"] = {}
                    self.index.flush()
                finally:
                    # Reopen real partition
                    self.dfds[part]["fd"] = util.create_open(orig_part)

    def getHeader(self):
        return self.index.header
