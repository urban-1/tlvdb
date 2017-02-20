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

## Hacking

I am afraid you will have to look into the [tests](tests) folder for now. A high
level usage example would be [test_objdb.py](tests/test_objdb.py) showing how
you can serialize and store custom objects. A middle level use with raw TLV items
can be found in `test_tlvdb.py` and low-level TLV packing/unpacking examples in
`test_tlv.py`
