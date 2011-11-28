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
    'FIS_TYPE_REG_H2D' : 0x27,
    'FIS_TYPE_REG_D2H' : 0x34,
    'FIS_TYPE_DMA_ACT' : 0x39,
    'FIS_TYPE_DMA_SETUP' : 0x41,
    'FIS_TYPE_DATA' : 0x46,
    'FIS_TYPE_BIST' : 0x58,
    'FIS_TYPE_PIO_SETUP' : 0x5F,
    'FIS_TYPE_DEV_BITS' : 0xA1
}

FIS_TYPES.update(dict((v,k) for k, v in FIS_TYPES.iteritems()))
