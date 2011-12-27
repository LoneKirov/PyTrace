class XgigCommand(object):
    def __init__(self, data):
        self.data = data

    def sTime(self):
        return self.data["metadata"]["sTimestamp"]

    def eTime(self):
        return self.data["metadata"]["eTimestamp"]

    def id(self):
        return self.data["metadata"]["id"]

    def port(self):
        return self.data["metadata"]["port"]

    def eventData(self):
        return self.data["eventData"]

    def getType(self):
        if "sata" in self.data:
            return "sata"
        elif "scsi" in self.data:
            return "scsi"
        else:
            return None
