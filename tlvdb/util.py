import os

def create_open(fname, mode="r+b", buffering=-1):
    # fake open to create
    if not os.path.exists(fname):
        with open(fname, "a+b") as f:
            pass

    return open(fname, "r+b", buffering=buffering)
