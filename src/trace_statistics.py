import csv
import copy

class Field(object):
    def __init__(self, name):
        self.__name = name

    def name(self):
        return self.__name

class Time(Field):
    def __init__(self, isStart):
        self.__isStart = isStart
        name = 'Start Time' if isStart else 'End Time'
        super(Time, self).__init__(name)
    def __call__(self, prev, cur, next):
        return cur.sTime() if self.__isStart else cur.eTime() 

class ID(Field):
    def __init__(self, isStart):
        self.__isStart = isStart
        name = 'ID' if isStart else 'End ID'
        super(ID, self).__init__(name)
    def __call__(self, prev, cur, next):
        return cur.start().eid if self.__isStart else cur.end().eid

class LBA(Field):
    def __init__(self):
        super(LBA, self).__init__('LBA')
    def __call__(self, prev, cur, next):
        return cur.start().lba

class FUA(Field):
    def __init__(self):
        super(FUA, self).__init__('FUA')
    def __call__(self, prev, cur, next):
        return 'Y' if hasattr(cur.start(), "fua") and cur.start().fua else 'N'

class Length(Field):
    def __init__(self):
        super(Length, self).__init__('length')
    def __call__(self, prev, cur, next):
        return cur.start().sectorCount if hasattr(cur.start(), "sectorCount") else 0

class InterCmdTime(Field):
    def __init__(self):
        super(InterCmdTime, self).__init__('InterCmdTime')
    def __call__(self, prev, cur, next):
        return 0 if prev is None else cur.sTime() - prev.sTime()

class CCT(Field):
    def __init__(self):
        super(CCT, self).__init__('CCT')
    def __call__(self, prev, cur, next):
        return cur.eTime() - cur.sTime()

class qCCT(Field):
    def __init__(self):
        super(qCCT, self).__init__('qCCT')
    def __call__(self, prev, cur, next):
        return cur.eTime() - prev.eTime() if prev != None else cur.eTime() - cur.sTime()

class CommandType(Field):
    def __init__(self):
        super(CommandType, self).__init__('Cmd')
    def __call__(self, prev, cur, next):
        return cur.cmdType

class qDepth(Field):
    def __init__(self):
        super(qDepth, self).__init__('qDepth')
    def __call__(self, prev, cur, next):
        return cur.start().qDepth if cur.queued else 0

ALL_FIELDS = [
        Time(True), Time(False), ID(True), ID(False), CommandType(),
        InterCmdTime(), LBA(), Length(), FUA(), CCT(), qCCT(), qDepth()
    ]

def commandsToStats(cmds, fields = ALL_FIELDS):
    fields = map(lambda f: copy.deepcopy(f), fields)

    def cmdIter(cmds):
        prev = None
        cur = None
        next = None
        for cmd in cmds:
            prev = cur
            cur = next
            next = cmd
            if cur is not None:
                yield [prev, cur, next]
        prev = cur
        cur = next
        next = None
        yield [prev, cur, next]

    for t in cmdIter(cmds):
        yield dict(map(lambda f: (f.name(), f(t[0], t[1], t[2])), fields))

def commandsToStatCSV(fName, cmds, fields = ALL_FIELDS):
    with open(fName, 'wb') as f:
        writer = csv.writer(f, delimiter=',', quotechar='|', quoting=csv.QUOTE_MINIMAL)
        writer.writerow(map(lambda f: f.name(), fields))

        for stat in commandsToStats(cmds, fields):
            writer.writerow(map(lambda f: stat[f.name()], fields))
