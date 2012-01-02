from xgig import XgigCommand

COMMANDS = {
    'SMART': 0xB0,
    'READ_FPDMA_QUEUED' : 0x60,
    'WRITE_FPDMA_QUEUED' : 0x61,
    'DATA_SET_MANAGEMENT' : 0x06,
    'FLUSH_CACHE_EXT' : 0xEA
}
COMMANDS.update(dict((v,k) for k, v in COMMANDS.iteritems()))

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
FIS_TYPES.update(dict((v,k) for k, v in FIS_TYPES.iteritems()))

SMART = {
    'READ_DATA' : 0xD0,
    'RETURN_STATUS' : 0xDA
}
SMART.update(dict((v,k) for k, v in SMART.iteritems()))

def getAndAssertKnown(v, m):
    assert v in m, "Unknown value %s" % hex(v)
    return m[v]

class FISCommand(XgigCommand):
    def fisType(self):
        return self.eventData()[4]

def parseLBA(data):
    lba = long(0)
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
    def lba(self):
        return parseLBA(self.eventData())

    def command(self):
        return self.eventData()[6]

class FISRegD2H(FISCommand):
    def lba(self):
        return parseLBA(self.eventData())

class FISSetDeviceBits(FISCommand):
    def acts(self):
        data = self.eventData()
        act = long(0)
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
        return acts

class Smart(FISRegH2D):
    def feature(self):
        return self.eventData()[7]

class WriteFPDMAQueued(FISRegH2D):
    def isWrite(self):
        return True

    def sectorCount(self):
        data = self.eventData()
        count = int(0)
        count = count | self.eventData()[15]
        count = count << 8
        count = count | self.eventData()[7]
        return count

    def queueTag(self):
        return self.eventData()[16] >> 3

    def fua(self):
        mask = 1 << 7
        return (self.eventData()[11] & mask) == mask

class ReadFPDMAQueued(FISRegH2D):
    def isRead(self):
        return True

    def sectorCount(self):
        data = self.eventData()
        count = int(0)
        count = count | self.eventData()[15]
        count = count << 8
        count = count | self.eventData()[7]
        return count

    def queueTag(self):
        return self.eventData()[16] >> 3

    def fua(self):
        mask = 1 << 7
        return (self.eventData()[11] & mask) == mask

def parseCommands(reader):
    commands = []
    inFlightQueued = {}
    lastQueued = None
    inFlightUnqueued = None

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
                feature = getAndAssertKnown(smart.feature(), SMART)
                if feature == 'READ_DATA':
                    yield [ smart ]
                elif feature == 'RETURN_STATUS':
                    assert inFlightUnqueued is None
                    inFlightUnqueued = [ smart ]
            elif command == 'WRITE_FPDMA_QUEUED':
                write = WriteFPDMAQueued(e)
                inFlightQueued[write.queueTag()] = [ write ]
                lastQueued = write
            elif command == 'READ_FPDMA_QUEUED':
                read = ReadFPDMAQueued(e)
                inFlightQueued[read.queueTag()] = [ read ]
                lastQueued = read

        elif 'FIS_REG_D2H' == fisType:
            fisRegD2H = FISRegD2H(e)
            if inFlightUnqueued is not None:
                assert inFlightUnqueued[0].lba() == fisRegD2H.lba()
                inFlightUnqueued.append(fisRegD2H)
                yield inFlightUnqueued
                inFlightUnqueued = None
            elif lastQueued is not None:
                inFlightQueued[lastQueued.queueTag()].append(fisRegD2H)
                lastQueued = None
        elif 'FIS_DEV_BITS' == fisType:
            bits = FISSetDeviceBits(e)
            for act in bits.acts():
                cmd = inFlightQueued[act]
                del inFlightQueued[act]
                cmd.append(bits)
                yield cmd
