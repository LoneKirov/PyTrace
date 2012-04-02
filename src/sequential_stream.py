from itertools import takewhile, zip_longest, filterfalse

class Detector(object):
    def __init__(self, cmds, attrName = 'stream', tDelta = 1000):
        self.__attrName = attrName
        self.__tDelta = tDelta
        self.__cmds = cmds
        self.__nextStreamID = 0
        pass

    def __iter__(self):
        inFlight = []
        expired = []
        issued = []

        for cmd in self.__cmds:
            issued.append(cmd)
            def isExpired(x):
                return (cmd.sTime() - x.eTime() > self.__tDelta * 1000) or hasattr(cmd, self.__attrName)
            for e in list(filter(isExpired, inFlight)):
                inFlight.remove(e)
                expired.append(e)

            expired.sort(key=lambda c: c.sTime())
            for t in list(takewhile(lambda c: c[0] == c[1], zip(expired, issued))):
                expired.remove(t[0])
                issued.remove(t[0])
                yield t[0]
            for c in filter(lambda x: hasattr(x.start(), "sectorCount") and cmd.start().lba == x.start().lba + x.start().sectorCount, inFlight):
                if hasattr(c, self.__attrName):
                    setattr(cmd, self.__attrName, getattr(c, self.__attrName))
                else:
                    setattr(c, self.__attrName, self.__nextStreamID)
                    setattr(cmd, self.__attrName, getattr(c, self.__attrName))
                    self.__nextStreamID += 1
                break
            inFlight.append(cmd)

        for c in issued:
            yield c
