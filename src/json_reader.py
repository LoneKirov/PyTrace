import platform
import mmap
import json

class JsonReader:
    def __init__(self, filename, channels=[0,1,2,3]):
        self.f = open(filename, "rb")
        self.channels = channels

    def __del__(self):
        self.f.close()

    def __iter__(self):
        map = mmap.mmap(self.f.fileno(), 0, mmap.MAP_PRIVATE)
        line = map.readline()
        while line:
            o = json.loads(line)
            if o["port"] in self.channels:
                yield o
            line = map.readline()
        map.close()

        raise StopIteration
