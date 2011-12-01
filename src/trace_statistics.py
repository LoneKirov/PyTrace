import csv

class CommandChain:
    def __init__(self, start, end=None):
        self.start = start
        self.chain = [ start ]
        if end is not None:
            self.chain.append(end)
        self.end = end

    def addCommand(self, c):
        self.chain.append(c)

    def setEnd(self, e):
        self.chain.append(e)
        self.end = e

class CommandStatistics:
    def __init__(self, prev, chain):
        self.prev = prev
        self.chain = chain

    def sTime(self):
        return self.chain.start["sTimestamp"]

    def eTime(self):
        if self.chain.end is None:
            return 0
        else:
            return self.chain.end["sTimestamp"]

    def sID(self):
        return self.chain.start["id"]

    def eID(self):
        if self.end is None:
            return ""
        else:
            return self.chain.end["id"]

    def q(self):
        #TODO: Figure out what to do here
        return "q"

    def cmd(self):
        #TODO: Figure out what to do here
        return ""

    def interCmdTime(self):
        #TODO: Figure out what to do here
        return 0

    def lba(self):
        return self.chain.start["FIS_REG_H2D"]["lba"]

    def length(self):
        #TODO: Figure out what to do here
        return 0

    def alignment(self):
        #TODO: Figure out what to do here
        return ""

    def fua(self):
        #TODO: Figure out what to do here
        return ""

    def cct(self):
        return self.eTime() - self.sTime()

    def qcct(self):
        #TODO: Figure out what to do here
        return self.cct()

    def qDepth(self):
        #TODO: Figure out what to do here
        return 0

    def eqDepth(self):
        #TODO: Figure out what to do here
        return 0

    def fifoPos(self):
        #TODO: Figure out what to do here
        return 0

    def seq(self):
        #TODO: Figure out what to do here
        return ""

    def seqNum(self):
        #TODO: Figure out what to do here
        return ""

    def cacheHit(self):
        #TODO: Figure out what to do here
        return ""

def commandsToCSV(fName, cmds):
    with open(fName, 'wb') as f:
        writer = csv.writer(f, delimiter=',', quotechar='|', quoting=csv.QUOTE_MINIMAL)
        writer.writerow(['Start Time', 'End Time', 'LBA'])

        prev = None
        for cmd in cmds:
            stat = CommandStatistics(prev, cmd)
            writer.writerow([stat.sTime(), stat.eTime(), stat.lba()])
            prev = cmd.end
