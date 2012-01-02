import csv
import copy

class Field(object):
    def __init__(self, name, valueFn):
        self._name = name
        self._valueFn = valueFn

    def name(self):
        return self._name

    def value(self, prev, cur, next):
        return self._valueFn(self, prev, cur, next)

class Time(Field):
    def __init__(self, isStart):
        name = 'Start Time' if isStart else 'End Time'
        def valueFn(self, prev, cur, next):
            return cur[0].sTime() if isStart else cur[len(cur) - 1].sTime() 
        super(Time, self).__init__(name, valueFn)

class ID(Field):
    def __init__(self, isStart):
        name = 'ID' if isStart else 'End ID'
        def valueFn(self, prev, cur, next):
            return cur[0].id() if isStart else cur[len(cur) - 1].id() 
        super(ID, self).__init__(name, valueFn)

class LBA(Field):
    def __init__(self):
        super(LBA, self).__init__('LBA', lambda self, prev, cur, next: cur[0].lba())

class FUA(Field):
    def __init__(self):
        super(FUA, self).__init__('FUA',
                lambda self, prev, cur, next:
                    'Y' if hasattr(cur[0], "fua") and cur[0].fua() else 'N'
            )

class Length(Field):
    def __init__(self):
        super(Length, self).__init__('length',
                lambda self, prev, cur, next:
                    cur[0].sectorCount() if hasattr(cur[0], "sectorCount") else 0
            )

class InterCmdTime(Field):
    def __init__(self):
        def valueFn(self, prev, cur, next):
            return 0 if prev is None else cur[0].sTime() - prev[0].sTime()
        super(InterCmdTime, self).__init__('InterCmdTime', valueFn)

class CCT(Field):
    def __init__(self):
        super(CCT, self).__init__('CCT',
                lambda self, prev, cur, next:
                    cur[len(cur) - 1].sTime() - cur[0].sTime()
            )

class CommandType(Field):
    def __init__(self):
        def valueFn(self, prev, cur, next):
            if hasattr(cur[0], 'isRead') and cur[0].isRead():
                return 'R'
            elif hasattr(cur[0], 'isWrite') and cur[0].isWrite():
                return 'W'
            else:
                return 0
        super(CommandType, self).__init__('Cmd', valueFn)

ALL_FIELDS = [
        Time(True), Time(False), ID(True), ID(False), CommandType(),
        InterCmdTime(), LBA(), Length(), FUA(), CCT()
    ]

def commandsToCSV(fName, cmds, fields = ALL_FIELDS):
    fields = map(lambda f: copy.deepcopy(f), fields)

    with open(fName, 'wb') as f:
        writer = csv.writer(f, delimiter=',', quotechar='|', quoting=csv.QUOTE_MINIMAL)
        writer.writerow(map(lambda f: f.name(), fields))

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
            writer.writerow(map(lambda f: f.value(t[0], t[1], t[2]), fields))
