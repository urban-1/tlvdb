import struct
import logging

lg = logging.getLogger("tlv")

from tlvdb.tlverrors import TlvSpecError


H=2**16
I=2**32
Q=2**64

class BaseIO(object):

    def __init__(self, fd):
        self.fd = fd

    def seek(self, pos, whence=0):

        if self.fd is None:
            raise RuntimeError("Not initialized read")

        self.fd.seek(pos, whence)

    def setSource(self, fd):
        self.fd = fd

    def read(self, pos, seek=True):
        raise NotImplementedError("read()")

    def write(self, pos, seek=True):
        raise NotImplementedError("write()")

class IPackable(object):
    """
    Generic packable interface - object provide pack and unpack methods
    """

    indexed = []
    """Indexed keys"""

    packaged = []
    """Packaged attributes"""

    @classmethod
    def valueToTLV(self, value):
        if isinstance(value, IPackable):
            return value.toTLV()
        elif type(value) == list:
            t = []
            for i in value:
                t.append(IPackable.valueToTLV(i))
            return TLV(t)
        elif type(value) == dict:
            d = {}
            for k, v in value.items():
                t[IPackable.valueToTLV(k)] = t[IPackable.valueToTLV(v)]
            return TLV(d)
        else:
            return TLV(value)

    def toTLV(self):
        t = {}
        for attr in self.__class__.packaged:
            if not hasattr(self, attr):
                continue

            value = getattr(self, attr)
            t[TLV(attr)] = IPackable.valueToTLV(value)

        return TLV(t)


    def pack(self, tab=""):
        return self.toTLV().pack()

    def unpack(self, fd):
        """
        Unpack from the given file descriptor and return self
        """
        t = TLV()
        t.unpack(fd)
        decoded = t.getDecodedValue()
        print(decoded)
        for attr in self.__class__.packaged:
            # Convert to bytes if needed
            attr_bytes = attr
            try:
                attr_bytes = attr.encode("ascii")
            except:
                pass
            if attr_bytes not in decoded:
                lg.warning("Could not find attr=%s" % attr)
                continue

            lg.debug("Unpacking %s" % attr)
            setattr(self, attr, decoded[attr_bytes])

        return self
        raise NotImplementedError("You need to implement 'unpack()' ...")

    def __str__(self):
        """Basic representation of packable attributes"""
        s = "%s\n" % self.__class__.__name__
        for attr in self.__class__.packaged:
            s = "%s   - %s: %s\n" % (s, attr, getattr(self, attr))

        return s

class TLV(BaseIO, IPackable):

    def __init__(self, value=None, fd=None):
        super(TLV, self).__init__(fd)
        self.value = value
        if self.value is not None:
            self._autoSetup()

    def setValue(self, value):
        self.value = value
        if self.value is not None:
            self._autoSetup()

    def getDecodedValue(self):
        """Get the TLV value decoded to python objects"""
        if type(self.value) == dict:
            r = {}
            for k, v in self.value.items():
                r[k.getDecodedValue()] = v.getDecodedValue()
            return r
        elif type(self.value) == list:
            # list of TLVs
            r = []
            for i in self.value:
                r.append(i.getDecodedValue())
            return r

        return self.value

    def __hash__(self):
        return hash((self.type, self.value, self.length))

    def __eq__(self, other):
        return (self.type, self.value, self.length) == (other.type, other.value, other.length)

    def __ne__(self, other):
        # Not strictly necessary, but to avoid having both x==y and x!=y
        # True at the same time
        return not(self == other)

    def _autoSetup(self):

        # Defaults
        self.length = 1
        self.type = None


        t = type(self.value)

        if t == list or t == set:
            self.length = len(self.value)
            self.type = b"T"
        elif t == dict:
            self.length = len(self.value)
            self.type = b"K"
        elif t == int:
            tmpv = abs(self.value)
            if tmpv < 256:
                self.type = b"B"
            elif tmpv < H:
                self.type = b"H"
            elif tmpv < I:
                self.type = b"I"
            elif tmpv < Q:
                self.type = b"Q"

            # sign
            if self.value < 0:
                self.type = self.type.lower()

        elif t == float:
            self.type = b"d"
        elif t == str or t == bytes:
            self.type = b"s"

            # Assume all string are ascii... if you dont like it give me bytes...
            try:
                if type(self.value) == str:
                    self.value = self.value.encode("ascii")
            except:
                pass

            self.length = len(self.value)
        else:
            raise RuntimeError("Unsupported type for value '%s'" % str(self.value))

        # self.type = self.type.encode("ascii")


    def size(self, pos, seek=True):
        if seek:
            self.seek(pos)

        lg.debug("Sizing from %d" % pos)

        data = self.fd.read(1)
        self.type = struct.unpack("<c", data)[0]
        data_len = 1


        # Lists! recursive
        if self.type == "T" or self.type == b"T":
            data = self.fd.read(1)
            self.length = struct.unpack("<B", data)[0]
            data_len += 1

            lg.debug("Sizing type=%s, length=%d" % (self.type, self.length))
            pos += 2
            for i in range(0, self.length):
                tmp = TLV(fd=self.fd)
                s = tmp.size(pos)
                data_len += s
                pos += s
        elif self.type == "K" or self.type == b"K":
            data = self.fd.read(1)
            self.length = struct.unpack("<B", data)[0]
            data_len += 1

            lg.debug("Sizing type=%s, length=%d" % (self.type, self.length))
            self.value = {}
            pos += 2
            for i in range(0, self.length):
                key = TLV(fd=self.fd)
                s = key.size(pos)
                data_len += s
                pos += s

                val = TLV(fd=self.fd)
                s = val.size(pos)
                data_len += s
                pos += s

        elif self.type == "s" or self.type == b"s":
            data = self.fd.read(1)
            self.length = struct.unpack("<B", data)[0]
            data_len += 1

            lg.debug("Sizing String type=%s, length=%d" % (self.type, self.length))
            data_len += self.length

        # Load
        else:
            lg.debug("Sizing type=%s" % (self.type))

            spec = "<%s" % self.type.decode("ascii")
            lg.debug("        spec=%s" % (spec))

            self.length = struct.calcsize(spec)
            lg.debug("        length=%d" % (self.length))
            data_len += self.length

        lg.debug("Found size=%d" % data_len)
        return data_len

    def unpack(self, fd):
        """
        Unpack from the given file descriptor and return self
        """
        self.fd = fd
        # Do not seek... just start reading
        self.read(self.fd.tell(), False)
        return self

    def read(self, pos, seek=True):
        if seek:
            self.seek(pos)

        lg.debug("Reading from %d" % pos)

        data = self.fd.read(1)
        self.type = struct.unpack("<c", data)[0]
        data_len = 1

        # Lists! recursive
        if self.type == "T" or self.type == b"T":
            data = self.fd.read(1)
            self.length = struct.unpack("<B", data)[0]
            data_len += 1

            lg.debug("Reading type=%s, length=%d" % (self.type, self.length))
            self.value = []
            pos += 2
            for i in range(0, self.length):
                tmp = TLV(fd=self.fd)
                dl = tmp.read(pos, seek)
                self.value.append(tmp)
                data_len += dl
                pos += dl
        elif self.type == "K" or self.type == b"K":
            data = self.fd.read(1)
            self.length = struct.unpack("<B", data)[0]
            data_len += 1

            lg.debug("Reading type=%s, length=%d" % (self.type, self.length))
            self.value = {}
            pos += 2
            for i in range(0, self.length):
                key = TLV(fd=self.fd)
                klen = key.read(pos, seek)
                data_len += klen
                pos += klen

                val = TLV(fd=self.fd)
                vlen = val.read(pos, seek)
                data_len += vlen
                pos += vlen

                self.value[key] = val
        elif self.type == "s" or self.type == b"s":
            data = self.fd.read(1)
            self.length = struct.unpack("<B", data)[0]
            data_len += 1

            lg.debug("Reading String type=%s, length=%d" % (self.type, self.length))
            data = self.fd.read(self.length)
            data_len += self.length
            self.value = struct.unpack("%ds" % self.length, data)[0]
        # Load
        else:
            lg.debug("Reading type=%s" % (self.type))

            spec = "<%s" % self.type.decode("ascii")
            lg.info("        spec=%s" % (spec))

            # This validates the spec, so control it!
            try:
                self.length = struct.calcsize(spec)
            except struct.error as e:
                raise TlvSpecError(
                    "While reading type=%s and spec=%s" % (self.type, spec),
                    str(e)
                )
            lg.debug("        length=%d" % (self.length))
            data = self.fd.read(self.length)
            data_len += self.length
            self.value = struct.unpack(spec, data)[0]

        lg.debug("Read %d" % data_len)
        return data_len

    def pack(self, tab=""):
        nexttab = "%s   " % tab
        if self.type == "T" or self.type == b"T":
            lg.debug("%sPacking List value as %d%s" % (tab, self.length, self.type))
            data = struct.pack("<cB", self.type, self.length)
            for i in self.value:
                data += i.pack(nexttab)
        elif self.type == "K" or self.type == b"K":
            lg.debug("%sPacking Dict value as %d%s" % (tab, self.length, self.type))
            data = struct.pack("<cB", self.type, self.length)
            for k, v in self.value.items():
                data += k.pack(nexttab) + v.pack(nexttab)
        elif self.type == "s" or self.type == b"s":
            try:
                self.length = len(self.value)
                spec = "<cB%ds" % self.length
                lg.debug("%sPacking String value as %d%s" % (tab, self.length, self.type))
                data = struct.pack(spec, self.type, self.length, self.value)
            except struct.error as e:
                raise TlvSpecError(
                    "While packing spec=%s and value=%s" % (spec, self.value),
                    str(e)
                )
        else:
            lg.debug("%sPacking Generic value as <c%s" % (tab, self.type.decode("ascii")))
            data = struct.pack("<c%s" % self.type.decode("ascii"), self.type, self.value)

        return data

    def write(self, pos, seek=True):
        if seek:
            self.seek(pos)

        data = self.pack()

        try:
            lg.debug("Writing type=%s, length=%d, data=%s @ pos=%d" % (self.type, self.length, data.encode("hex"), pos))
        except:
            lg.debug("Writing type=%s, length=%d, data=%s @ pos=%d" % (self.type, self.length, data, pos))

        lg.debug("Writing data length = %d" % len(data))
        self.fd.write(data)

        return pos+len(data)

    def __str__(self):
        tmpval = str(self.value)

        if self.type == "T" or self.type == b"T":
            tmpval = [t.__str__() for t in self.value]
        elif self.type == "K" or self.type == b"K":
            tmpval = ["%s:%s" % (k.__str__(), v.__str__()) for k,v in self.value.items()]

        return "(%s, %d, %s)" % (self.type, self.length, tmpval)
