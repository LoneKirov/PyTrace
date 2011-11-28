class Command:
    def __init__(self, prev, start=(), end=()):
        self.prev = prev
        self.start = start
        self.end = end

    def sTime(self):
        return self.start["sTimestamp"]

    def eTime(self):
        return self.end["eTimestamp"]

    def sID(self):
        return self.start["id"]

    def eID(self):
        return self.end["id"]

    def q(self):
        #TODO: Figure out what to do here
        return "q"

    def cmd(self):
        #TODO: Figure out what to do here
        return ""

    def interCmdTime(self):
        return self.sTime() - prev.eTime()

    def lba(self):
        return self.start["lba"]

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
