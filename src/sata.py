from xgig import XgigEvent, ParsedCommand
from itertools import takewhile, dropwhile
import logging

COMMANDS = {
    'SMART': 0xB0,
    'READ_FPDMA_QUEUED' : 0x60,
    'WRITE_FPDMA_QUEUED' : 0x61,
    'DATA_SET_MANAGEMENT' : 0x06,
    'WRITE_DMA_EXT' : 0x35,
    'FLUSH_CACHE_EXT' : 0xEA
}
COMMANDS.update(dict((v,k) for k, v in COMMANDS.items()))

FIS_TYPES = {
    'UNKNOWN' : 0x00,
    'FIS_REG_H2D' : 0x27,
    'FIS_REG_D2H' : 0x34,
    'FIS_DMA_ACT' : 0x39,
    'FIS_DMA_SETUP' : 0x41,
    'FIS_DATA' : 0x46,
    'FIS_BIST' : 0x58,
    'FIS_PIO_SETUP' : 0x5F,
    'FIS_DEV_BITS' : 0xA1
}
FIS_TYPES.update(dict((v,k) for k, v in FIS_TYPES.items()))

SMART = {
    'READ_DATA' : 0xD0,
    'RETURN_STATUS' : 0xDA
}
SMART.update(dict((v,k) for k, v in SMART.items()))

def getAndAssertKnown(v, m):
    assert v in m, "Unknown value %s" % hex(v)
    return m[v]

class FISCommand(XgigEvent):
    def __init__(self, event):
        super().__init__(event)
        self.fisType = self.eventData()[4]

def parseLBA(data):
    lba = int(0)
    lba = lba | data[14]
    lba = lba << 8
    lba = lba | data[13]
    lba = lba << 8
    lba = lba | data[12]
    lba = lba << 8
    lba = lba | data[10]
    lba = lba << 8
    lba = lba | data[9]
    lba = lba << 8
    lba = lba | data[8]
    return lba


class FISRegH2D(FISCommand):
    def __init__(self, event):
        super().__init__(event)
        self.lba = parseLBA(self.eventData())
        self.command = self.eventData()[6]

class FISRegD2H(FISCommand):
    def __init__(self, event):
        super().__init__(event)
        self.lba = parseLBA(self.eventData())

class FISSetDeviceBits(FISCommand):
    def __init__(self, event, eqDepth):
        super().__init__(event)
        self.eqDepth = eqDepth
        data = self.eventData()
        act = int(0)
        act = act | data[11]
        act = act << 8
        act = act | data[10]
        act = act << 8
        act = act | data[9]
        act = act << 8
        act = act | data[8]
        acts = []
        for x in range(32):
            if act & (1 << x):
                acts.append(x)
        self.acts = acts

class Smart(FISRegH2D):
    def __init__(self, event):
        super().__init__(event)
        self.feature = self.eventData()[7]

class WriteFPDMAQueued(FISRegH2D):
    def __init__(self, event, qDepth):
        super().__init__(event)
        self.qDepth = qDepth

        data = self.eventData()
        count = int(0)
        count = count | self.eventData()[15]
        count = count << 8
        count = count | self.eventData()[7]
        self.sectorCount = count
        self.queueTag = self.eventData()[16] >> 3
        mask = 1 << 7
        self.fua = (self.eventData()[11] & mask) == mask

class ReadFPDMAQueued(FISRegH2D):
    def __init__(self, event, qDepth):
        super().__init__(event)
        self.qDepth = qDepth

        data = self.eventData()
        count = int(0)
        count = count | self.eventData()[15]
        count = count << 8
        count = count | self.eventData()[7]
        self.sectorCount = count
        self.queueTag = self.eventData()[16] >> 3
        mask = 1 << 7
        self.fua = (self.eventData()[11] & mask) == mask

class FlushCacheExt(FISRegH2D):
    def __init__(self, event):
        super().__init__(event)
        self.lba = 0
        self.sectorCount = 0

class DataSetManagement(FISRegH2D):
    def __init__(self, event):
        super().__init__(event)
        self.lba = 0
        self.sectorCount = 0

class WriteDMAExt(FISRegH2D):
    def __init__(self, event):
        super().__init__(event)

class Parser(object):
    def __init__(self, events):
        self.__events = events
        self.__commands = []
        self.__inFlightQueued = {}
        self.__lastQueued = None
        self.__inFlightUnqueued = None
        self.__prevEvent = None

    def __iter__(self):
        for e in self.__events:
            if "sata" not in e:
                continue
            sata = e["sata"]

            fisType = sata["fisType"]
            fisType = getAndAssertKnown(fisType, FIS_TYPES)
            self.handle(fisType, e)

            self.__commands.sort(key=lambda c: c.sTime())
            for c in takewhile(lambda c: c.done, self.__commands):
                yield c
            self.__commands[:] = list(dropwhile(lambda c: c.done, self.__commands))
            
        self.__commands.sort(key=lambda c: c.sTime())
        for cmd in self.__commands:
            yield cmd

    def handle(self, t, e):
        h = getattr(self, t, None)
        self.LOGGER.warn("Unhandled %s (%s, %s)", t, e["metadata"]["id"], e["metadata"]["sTimestamp"]) if h is None else h(e)

    def FIS_REG_H2D(self, e):
        sata = e["sata"]
        command = sata["command"]
        command = getAndAssertKnown(command, COMMANDS)
        self.handle(command, e)
        self.__prevEvent = XgigEvent(e)

    def SMART(self, e):
        smart = Smart(e)
        feature = getAndAssertKnown(smart.feature, SMART)
        if feature == 'READ_DATA':
            self.LOGGER.info("Ignoring SATA READ_DATA")
        elif feature == 'RETURN_STATUS':
            assert self.__inFlightUnqueued is None
            self.__inFlightUnqueued = ParsedCommand(events=[ smart ], cmdType='-', prevEvent=self.__prevEvent)
            self.__commands.append(self.__inFlightUnqueued)

    def WRITE_FPDMA_QUEUED(self, e):
        write = WriteFPDMAQueued(e, len(self.__inFlightQueued))
        self.__inFlightQueued[write.queueTag] = ParsedCommand(events=[ write ], queued=True, cmdType='W', prevEvent=self.__prevEvent)
        self.__commands.append(self.__inFlightQueued[write.queueTag])
        self.__lastQueued = write.queueTag

    def READ_FPDMA_QUEUED(self, e):
        read = ReadFPDMAQueued(e, len(self.__inFlightQueued))
        self.__inFlightQueued[read.queueTag] = ParsedCommand(events=[ read ], queued=True, cmdType='R', prevEvent=self.__prevEvent)
        self.__commands.append(self.__inFlightQueued[read.queueTag])
        self.__lastQueued = read.queueTag

    def FLUSH_CACHE_EXT(self, e):
        flush = FlushCacheExt(e)
        assert self.__inFlightUnqueued is None
        self.__inFlightUnqueued = ParsedCommand(events=[ flush ], cmdType='F', prevEvent=self.__prevEvent)
        self.__commands.append(self.__inFlightUnqueued)

    def DATA_SET_MANAGEMENT(self, e):
        dsm = DataSetManagement(e)
        assert self.__inFlightUnqueued is None
        self.__inFlightUnqueued = ParsedCommand(events=[ dsm ], cmdType='-', prevEvent=self.__prevEvent)
        self.__commands.append(self.__inFlightUnqueued)

    def FIS_REG_D2H(self, e):
        fisRegD2H = FISRegD2H(e)
        if self.__inFlightUnqueued is not None:
            assert self.__inFlightUnqueued.events[0].lba == fisRegD2H.lba
            self.__inFlightUnqueued.events.append(fisRegD2H)
            self.__inFlightUnqueued.done = True
            self.__inFlightUnqueued = None
            self.__prevEvent = XgigEvent(e)
        elif self.__lastQueued is not None:
            self.__inFlightQueued[self.__lastQueued].events.append(fisRegD2H)
            self.__lastQueued = None
        else:
            self.LOGGER.warn("Unhandled FIS_REG_D2H (%s, %s)", e["metadata"]["id"], e["metadata"]["sTimestamp"])

    def FIS_DEV_BITS(self, e):
        bits = FISSetDeviceBits(e, len(self.__inFlightQueued) - 1)
        for act in bits.acts:
            cmd = self.__inFlightQueued[act]
            del self.__inFlightQueued[act]
            cmd.events.append(bits)
            cmd.done = True
        self.__prevEvent = XgigEvent(e)

Parser.LOGGER = logging.getLogger(Parser.__name__)
