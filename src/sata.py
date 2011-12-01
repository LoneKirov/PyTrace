from trace_statistics import CommandChain

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

def parseCommands(reader):
    commands = []
    inFlightQueued = {}
    lastQueued = None
    inFlightUnqueued = None

    for e in reader:
        if 'FIS_REG_H2D' in e:
            data = e['FIS_REG_H2D']
            command = getAndAssertKnown(data['command'], COMMANDS)

            if command == 'SMART':
                feature = getAndAssertKnown(data['feature'], SMART)
                if feature == 'READ_DATA':
                    commands.append(CommandChain(e))
                elif feature == 'RETURN_STATUS':
                    assert inFlightUnqueued is None
                    inFlightUnqueued = CommandChain(e)
        elif 'FIS_REG_D2H' in e:
            data = e['FIS_REG_D2H']
            if inFlightUnqueued is not None:
                assert inFlightUnqueued.start['FIS_REG_H2D']['lba'] == data['lba']
                inFlightUnqueued.setEnd(e)
                commands.append(inFlightUnqueued)
                inFlightUnqueued = None

    return commands

def parseQueueNumber(e):
    return None
