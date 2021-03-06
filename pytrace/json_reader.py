import platform
import mmap
import json


class JsonReader(object):
    def __init__(self, filename, channels=[0,1,2,3]):
        self.f = open(filename, "rb")
        self.channels = channels

    def __del__(self):
        self.f.close()

    def __iter__(self):
        map = mmap.mmap(self.f.fileno(), 0, mmap.MAP_PRIVATE)
        line = map.readline()
        while line:
            o = json.loads(str(line, "UTF-8"))
            if o["metadata"]["port"] in self.channels:
                yield o
            line = map.readline()
        map.close()

        raise StopIteration
