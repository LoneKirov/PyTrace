from xgig import XgigEvent, ParsedCommand
from itertools import takewhile, dropwhile
import logging

COMMANDS = {
    'SMART': 0xB0,
    'READ_FPDMA_QUEUED' : 0x60,
    'WRITE_FPDMA_QUEUED' : 0x61,
    'DATA_SET_MANAGEMENT' : 0x06,
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
        super(FISCommand, self).__init__(event)
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
        super(FISRegH2D, self).__init__(event)
        self.lba = parseLBA(self.eventData())
        self.command = self.eventData()[6]

class FISRegD2H(FISCommand):
    def __init__(self, event):
        super(FISRegD2H, self).__init__(event)
        self.lba = parseLBA(self.eventData())

class FISSetDeviceBits(FISCommand):
    def __init__(self, event, eqDepth):
        super(FISSetDeviceBits, self).__init__(event)
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
        super(Smart, self).__init__(event)
        self.feature = self.eventData()[7]

class WriteFPDMAQueued(FISRegH2D):
    def __init__(self, event, qDepth):
        super(WriteFPDMAQueued, self).__init__(event)
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
        super(ReadFPDMAQueued, self).__init__(event)
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
        super(FlushCacheExt, self).__init__(event)
        self.lba = 0
        self.sectorCount = 0

class DataSetManagement(FISRegH2D):
    def __init__(self, event):
        super(DataSetManagement, self).__init__(event)
        self.lba = 0
        self.sectorCount = 0


def parseCommands(reader):
    commands = []
    inFlightQueued = {}
    lastQueued = None
    inFlightUnqueued = None
    prevEvent = None

    for e in reader:
        if "sata" not in e:
            continue
        sata = e["sata"]

        fisType = sata["fisType"]
        fisType = getAndAssertKnown(fisType, FIS_TYPES)
        if 'FIS_REG_H2D' == fisType:
            command = sata["command"]
            command = getAndAssertKnown(command, COMMANDS)

            if command == 'SMART':
                smart = Smart(e)
                feature = getAndAssertKnown(smart.feature, SMART)
                if feature == 'READ_DATA':
                    logging.info("Ignoring SATA READ_DATA")
                elif feature == 'RETURN_STATUS':
                    assert inFlightUnqueued is None
                    inFlightUnqueued = ParsedCommand(events=[ smart ], cmdType='-', prevEvent=prevEvent)
                    commands.append(inFlightUnqueued)
            elif command == 'WRITE_FPDMA_QUEUED':
                write = WriteFPDMAQueued(e, len(inFlightQueued))
                inFlightQueued[write.queueTag] = ParsedCommand(events=[ write ], queued=True, cmdType='W', prevEvent=prevEvent)
                commands.append(inFlightQueued[write.queueTag])
                lastQueued = write.queueTag
            elif command == 'READ_FPDMA_QUEUED':
                read = ReadFPDMAQueued(e, len(inFlightQueued))
                inFlightQueued[read.queueTag] = ParsedCommand(events=[ read ], queued=True, cmdType='R', prevEvent=prevEvent)
                commands.append(inFlightQueued[read.queueTag])
                lastQueued = read.queueTag
            elif command == 'FLUSH_CACHE_EXT':
                flush = FlushCacheExt(e)
                assert inFlightUnqueued is None
                inFlightUnqueued = ParsedCommand(events=[ flush ], cmdType='F', prevEvent=prevEvent)
                commands.append(inFlightUnqueued)
            elif command == 'DATA_SET_MANAGEMENT':
                dsm = DataSetManagement(e)
                assert inFlightUnqueued is None
                inFlightUnqueued = ParsedCommand(events=[ dsm ], cmdType='-', prevEvent=prevEvent)
                commands.append(inFlightUnqueued)
            else:
                logging.warn("Unhandled command %s (%s, %s)", command, e["metadata"]["id"], e["metadata"]["sTimestamp"])
            prevEvent = XgigEvent(e)

        elif 'FIS_REG_D2H' == fisType:
            fisRegD2H = FISRegD2H(e)
            if inFlightUnqueued is not None:
                assert inFlightUnqueued.events[0].lba == fisRegD2H.lba
                inFlightUnqueued.events.append(fisRegD2H)
                inFlightUnqueued.done = True
                inFlightUnqueued = None
                prevEvent = XgigEvent(e)
            elif lastQueued is not None:
                inFlightQueued[lastQueued].events.append(fisRegD2H)
                lastQueued = None
            else:
                logging.warn("Unhandled FIS_REG_D2H (%s, %s)", e["metadata"]["id"], e["metadata"]["sTimestamp"])
        elif 'FIS_DEV_BITS' == fisType:
            bits = FISSetDeviceBits(e, len(inFlightQueued) - 1)
            for act in bits.acts:
                cmd = inFlightQueued[act]
                del inFlightQueued[act]
                cmd.events.append(bits)
                cmd.done = True
            prevEvent = XgigEvent(e)
        else:
            logging.warn("Unhandled fisType %s (%s, %s)", fisType, e["metadata"]["id"], e["metadata"]["sTimestamp"])

        commands.sort(key=lambda c: c.sTime())
        for c in takewhile(lambda c: c.done, commands):
            yield c
        commands[:] = list(dropwhile(lambda c: c.done, commands))
            
    commands.sort(key=lambda c: c.sTime())
    for cmd in commands:
        yield cmd
