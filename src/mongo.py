import pymongo
import copy
import xgig

class MongoCaptureDatabase(object):
    def __init__(self, dbName, captureName, username, password):
        self.__dbName = dbName
        self.__captureName = captureName
        self.__username = username
        self.__password = password
        self.__connection = pymongo.Connection()

    def getDB(self):
        db = self.__connection[self.__dbName]
        return db

    def getEventsCollection(self):
        return self.getDB()["%s_events" % self.__captureName]

    def importEvents(self, events):
        eventsCollection = self.getEventsCollection()
        eventsCollection.drop()
        for e in events:
            eventsCollection.insert(e)
        eventsCollection.create_index("metadata.id")

    def getStatsCollection(self):
        return self.getDB()["%s_stats" % self.__captureName]

    def importStats(self, stats):
        statsCollection = self.getStatsCollection()
        statsCollection.drop()
        for stat in stats:
            statsCollection.insert(stat)

    def getCommandsCollection(self):
        return self.getDB()["%s_commands" % self.__captureName]

    def importCommands(self, cmds):
        cmdsCollection = self.getCommandsCollection()
        cmdsCollection.drop()
       
        for cmd in cmds:
            cmdsCollection.insert(self.serializeCommand(cmd))

    def serializeCommand(self, cmd):
        cmd = copy.deepcopy(cmd)
        for e in cmd.events:
            del e._event
        cmd.events = list(map(lambda e: e.__dict__, cmd.events))
        return cmd.__dict__

    def deserializeCommand(self, cmd):
        cmd = copy.deepcopy(cmd)
        oEvents = cmd["events"]
        cmd["events"] = list(map(
                lambda e: xgig.XgigCommand(self.getEventsCollection().find_one({"metadata.id" : e["eid"]})),
                oEvents))
        for i in range(len(oEvents)):
            cmd["events"][i].__dict__.update(**oEvents[i])

        pcmd = xgig.ParsedCommand()
        pcmd.__dict__.update(**cmd)
        return pcmd
