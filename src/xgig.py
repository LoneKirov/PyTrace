class XgigCommand(object):
    def __init__(self, event):
        self.__event = event

    def sTime(self):
        return self.__event["metadata"]["sTimestamp"]

    def eTime(self):
        return self.__event["metadata"]["eTimestamp"]

    def id(self):
        return self.__event["metadata"]["id"]

    def port(self):
        return self.__event["metadata"]["port"]

    def eventData(self):
        return self.__event["eventData"]

    def getType(self):
        if "sata" in self.__event:
            return "sata"
        elif "scsi" in self.__event:
            return "scsi"
        else:
            return None
