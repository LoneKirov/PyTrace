import pymongo

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

    def closeConnection(self):
        self.__connection.close()

    def getEventsCollection(self):
        return self.getDB()["%s_events" % self.__captureName]

    def importEvents(self, events):
        eventsCollection = self.getEventsCollection()
        eventsCollection.drop()
        for e in events:
            eventsCollection.insert(e)
        eventsCollection.create_index("metadata.id")
        self.closeConnection()

    def getStatsCollection(self):
        return self.getDB()["%s_stats" % self.__captureName]

    def importStats(self, stats):
        statsCollection = self.getStatsCollection()
        statsCollection.drop()
        for stat in stats:
            statsCollection.insert(stat)
        self.closeConnection()
