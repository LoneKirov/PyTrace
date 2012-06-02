class XgigEvent(object):
    def __init__(self, event):
        self._event = event
        self.sTime = event["metadata"]["sTimestamp"]
        self.eTime = event["metadata"]["eTimestamp"]
        self.eid = event["metadata"]["id"]
        self.port = event["metadata"]["port"]
        if "sata" in event:
            self.protocol = "sata"
        elif "scsi" in event:
            self.protocol = "scsi"
        else:
            self.protocol = None

    def event(self):
        return self._event

    def eventData(self):
        return self.event()["eventData"]

class ParsedCommand(object):
    def __init__(self, events=[], queued=False, cmdType=0, done=False, prevEvent=None):
        self.events = events
        self.queued = queued
        self.cmdType = cmdType
        self.done = done
        self.prevEvent = prevEvent

    def start(self):
        return self.events[0]
    def sTime(self):
        return self.start().sTime
    def end(self):
        return self.events[len(self.events) - 1]
    def eTime(self):
        return self.end().sTime
    def ack(self):
        if self.queued:
            assert len(self.events) == 3
            return self.events[1]
        else:
            return self.end()
    def ackTime(self):
        return self.ack().sTime
