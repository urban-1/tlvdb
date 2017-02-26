import os
import time
import signal
import logging as lg
# We are using simple Lock for individual partition fds since noone else is
# using them
from multiprocessing import Lock


from tlvdb import util
from tlvdb.tlv import TLV
from tlvdb.tlvindex import HashIndex
from tlvdb.tlverrors import *
from tlvdb.util import DelayedInterrupt
from tlvdb.tlvbtreeindex import BPlusTreeIndex

# x MB buffer
IO_BUFFER_LEN = 1000000


class TlvStorage(object):

    VERSION = 1
    """Storage version"""

    MAX_INDEXES = 10
    """Maximum number of cached indexes"""

    def __init__(self, index_file, vacuum_thres = 0.1, backfill=False):
        """
        :param str index_file: Path to the main index file
        :param bool backfill: Should we look for a better place to write? (not used)
        :param float vacuum_thres: Ratio of empty items over items in the index
                                   that will cause a vacuum to not abort
        """
        self.backfill = backfill
        self.vacuum_thres = vacuum_thres

        # Sort out files
        self.basename = os.path.basename(index_file)
        self.dirname = os.path.abspath(os.path.dirname(index_file))
        self.basename = self.basename[0:self.basename.index(".")]
        self.in_trance = False

        # open fds
        self.ifd = util.create_open(index_file)
        self.index = HashIndex(self.ifd)


        # Global storage lock required for vacuuming and creating
        self.lock = Lock()

        # data file descriptors
        with self.lock:
            self.dfds = []
            for p in range(0, self.getHeader().partitions):
                tmppath = "%s/%s.%d.dat" % (self.dirname, self.basename, p)
                # Open partition
                tmpfd = util.create_open(tmppath, "r+b", buffering=IO_BUFFER_LEN)
                self.dfds.append({
                    "fd": tmpfd,
                    "path": tmppath,
                    "lock": Lock()
                    })

            self.clean = True

        self.data_indexes = {}
        """
        A hash holding index per attribute The strcutre should be:
        name: {
            used: epoch,
            index: object
        }

        The oldest used index will be automatically dropped when a new index
        needs to be loaded
        """

    def _getDataFileEnd(self, part):
        """
        Caller is responsible of locking
        """
        try:
            return self.dfds[part]["last"]
        except:
            pass

        self.dfds[part]["fd"].seek(0,2)
        self.dfds[part]["last"] = self.dfds[part]["fd"].tell()
        return self.dfds[part]["last"]

    def _findAGoodPossiotion(self, part, size):
        return self._getDataFileEnd(part)

    def _handleIndexing(self, operation, tlvid, packable=None, pos=None):
        """
        Update this TlvStorage b+ indexes
        """
        # Go for the index we know of (like in a delete operation!)
        if packable is None:
            for attr, di in self.data_indexes.items():
                with di["index"].lock:
                    di["index"].handle(operation, tlvid)

                # Flush if needed
                if self.in_trance is False:
                    di["index"].flush()

            return

        # Adding or updating... we have a lot more info about the transaction
        for attr in packable.indexed:
            if attr not in self.data_indexes.keys():
                indexpath = "%s/%s.%s.dat" % (self.dirname, self.basename, attr)
                self.data_indexes[attr] = {
                    "used": time.time(),
                    "index": BPlusTreeIndex(indexpath)
                }

            with self.data_indexes[attr]["index"].lock:
                self.data_indexes[attr]["index"].handle(operation, tlvid, getattr(packable, attr), pos)
                if self.in_trance is False:
                    self.data_indexes[attr]["index"].flush()


    def beginTransaction(self):
        self.in_trance = True

    def endTransaction(self):
        self.in_trance = False
        self.index.flush()

        # Lock and flush all data fds (simple flush - no writing)
        for p in self.dfds:
            with p["lock"]:
                p["fd"].flush()

        # For all data indexes
        for attr, di in self.data_indexes.items():
            with di["index"].lock:
                
                # Skip already clean ones
                if di["index"].clean:
                    continue

                # This is a complete index re-write - call wisely
                di["index"].flush()


    def create(self, packable):
        """
        Create a new entry in the database from the given tlv
        """
        with self.index.lock:
            lg.info("create: Locked")
            # 1. Find next available ID
            nextid = self.index.nextid

            # 2. Find the best partition (TODO)
            part = 0

            # 3. Find next pos in data file (TODO: consult index)
            data = packable.pack()
            datalen = len(data)

            # Lock the partition
            with self.dfds[part]["lock"]:
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
                with self.dfds[part]["lock"]:
                    self.dfds[part]["fd"].flush()

            self._handleIndexing("create", nextid, packable, pos)

            lg.info("create: Releasing")
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

        # Lock the partition we are reading from
        #
        with self.dfds[part]["lock"]:
            self.dfds[part]["fd"].seek(pos)
            instance.unpack(self.dfds[part]["fd"])

        instance._tlvdb_id = tid
        instance._tlvdb_clean = True
        return instance

    def delete(self, tid, klass=None):
        """
        Delete an entry. If class is given, the deleted entry will be returned
        """
        # This will lock the index for reading...
        part, oldpos = self.index.delete(tid)

        if part is False:
            return False

        # we have deleted, check if we should return the old entry...
        ret = True
        if klass:
            instance = klass()

            # Lock and load
            with self.dfds[part]["lock"]:
                self.dfds[part]["fd"].seek(oldpos)
                ret = instance.unpack(self.dfds[part]["fd"])


        # Handle index
        self._handleEmptying(part, oldpos)
        self._handleIndexing("delete", tid)

        # In any case, flush index
        if self.in_trance is False:
            self.index.flush()

        return ret

    def update(self, obj):
        if not hasattr(obj, "_tlvdb_id"):
            raise WrongInstanceError()

        # Update is doing multiple operations on the index and thus should be
        # locked ALL the time... we dont want it changing in between...
        with self.index.lock:
            # Read original object
            old = self.read(obj._tlvdb_id)

            # get old index
            part, oldpos = self.index.get(obj._tlvdb_id)
            if not oldpos:
                raise IndexNotFoundError("Object with id=%d not found" % obj._tlvdb_id)

            new_data = obj.pack()
            old_data = old.pack()

            datalen = len(new_data)

            # See if we can fit it!
            pos = -1
            with self.dfds[part]["lock"]:
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
                if not pos: print(pos)
                self.dfds[part]["fd"].seek(pos)
                self.dfds[part]["fd"].write(new_data)
                self.dfds[part]["fd"].flush()

            # Update the index
            self.index.update(part, obj._tlvdb_id, pos)
            self.index.flush()

    def _handleEmptying(self, part, oldpos):
        """
        Called when something is moved or deleted
        """
        if self.backfill is True:
            # Log with detail
            with self.dfds[part]["lock"]:
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

        # Lock everything
        self.lock.acquire()
        self.index.lock.acquire()

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

            if items and self.vacuum_thres > empty/items:
                lg.info("Skipping ... partition less than threshold (thres=%f <> frag=%f)" % (self.vacuum_thres, empty/items))
                continue


            # Create swap
            swap_part = len(self.dfds)
            lg.info("Vacuum: Starting Partition %d" % swap_part)
            swap_path = "%s/%s.%d.dat" % (self.dirname, self.basename, swap_part)
            self.dfds.append({})
            self.dfds[swap_part]["last"] = 0
            self.dfds[swap_part]["lock"] = Lock()
            self.dfds[swap_part]["fd"] = open(swap_path, "wb")

            lg.info(" ... Vacuum: Starting ")

            # lock our partition pointers
            self.dfds[part]["lock"].acquire()

            # Lock swap
            self.dfds[swap_part]["lock"].acquire()

            for tid, pos in cont["index"].items():
                if pos == 0:
                    lg.warning("Skipping empty/deleted index?")
                    self.dfds[part]["lock"].release()
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

            # Release swap... all done for this partions (swap is per-part)
            self.dfds[swap_part]["lock"].release()

            del self.dfds[swap_part]

            # Clean up real partition
            self.dfds[part]["fd"].close()
            if "last" in self.dfds[part]:
                del self.dfds[part]["last"]

            # DANGEROUS PART: SHOULD NOT BE INTERUPTED
            orig_part = "%s/%s.%d.dat" % (self.dirname, self.basename, part)

            # with DelayedInterrupt(signal.SIGINT):
            try:
                os.rename(swap_path, orig_part)
            except:
                lg.critical("Failed to move packed parition...")
                self.index.reload()
            else:
                lg.info(" ...Done ")
                cont["empty"] = {}
                self.index.flush()
            finally:
                # Reopen real partition
                self.dfds[part]["fd"] = util.create_open(orig_part)

            # Time to release, this partition is done
            self.dfds[part]["lock"].release()


        # Be free...
        self.index.lock.release()
        self.lock.release()
        lg.info(" ... Vacuum: Stopped ")

    def getHeader(self):
        return self.index.header
