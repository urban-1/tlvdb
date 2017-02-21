# tlvdb

A TLV and file-based database implementation in Python

## Intro
### Why?

Have you ever asked how difficult is to implement a database?
What are the trade-offs? Why isn't a database out there that fits all?

Well... I did, so I am giving it a try

### Who is this project for?

I think that 'd be only me... but in general any developer that wants to
implement a custom database might find it a good starting point

## The basics

The main features are:

- TLV implemenation based on `struct` python module
- Binary file storage
- Hash index for items
- Thread-safe
- Defragmentation threshold


## Example Usage

```python
import os
import time
import unittest
import logging as lg

import tlvdb.util as util
from tlvdb.tlv import IPackable
from tlvdb.tlverrors import *
from tlvdb.tlvstorage import TlvStorage

class Person(IPackable):

    packaged = ["name", "numbers", "age"]

    def __init__(self, name="", numbers=[], age=-1):
        self.name = name
        self.numbers = numbers
        self.age = age

ROOT = os.path.abspath(os.path.dirname(os.path.dirname(__file__)))
IFILE = "%s/data/test_obj.idx" % ROOT
ts = TlvStorage(IFILE)

p = Person("Andreas", ["0712312312", "0897408123"], 30)
pid = ts.create(p)
p2 = ts.read(pid, klass=Person)
lg.debug("Got %s" % p2)
```

## Hacking

I am afraid you will have to look into the [tests](tests) folder for now. A high
level usage example would be [test_objdb.py](tests/test_objdb.py) showing how
you can serialize and store custom objects. A middle level use with raw TLV items
can be found in `test_tlvdb.py` and low-level TLV packing/unpacking examples in
`test_tlv.py`

## Performance

Performance is not a concern at this stage of this project... However, while
testing thread safety I found out that:

- 5-7 threads seems to be the most efficient setup
- python3 is at least 1ms/transaction faster than python2
- pypy is under-performing with +2ms and most importantly hits DEADLOCKS
- threading is b***h :)

[All testing done on a laptop, ubuntu 14, nfts partition via unittests]
