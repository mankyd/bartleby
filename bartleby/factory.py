from bsddb import db
from twisted.internet import defer, protocol
from twisted.python import log

from bartleby.protocol import BartlebyProtocol

class BartlebyFactory(protocol.ServerFactory):
    protocol = BartlebyProtocol

    def __init__(self):
        self._db = {}
        self._bdb = db.DB()
        self._bdb.open('test.db', 'bartleby', db.DB_HASH, db.DB_CREATE)

    def incr(self, key, val=1):
        self._db[key] = self._db.get(key, int(self._bdb.get(key, 0))) + val
        if val != 0 or not self._bdb.exists(key):
            self._bdb.put(key, str(self._db[key]))

        return defer.succeed(str(self._db[key]))

    def set(self, key, val=0):
        self._db[key] = val
        self._bdb.put(key, str(val))

        return defer.succeed(val)

    def add(self, key, val=0):
        if key in self._db:
            return defer.succeed(None)
        existing_val = self._bdb.get(key)
        if existing_val is not None:
            self._db[key] = int(existing_val)
            return defer.succeed(None)
        
        return self.set(key, val)

    def replace(self, key, val=0):
        if not key in self._db:
            existing_val = self._bdb.get(key)
            if existing_val is None:
                return defer.succeed(None)
            
        return self.set(key, val)

    def get(self, keys):
        result = {}
        for key in keys:
            if key in self._db:
                result[key] = self._db[key]
            else:
                val = self._bdb.get(key)
                if val is not None:
                    self._db[key] = int(val)
                    result[key] = int(val)
        return defer.succeed(result)

    def delete(self, key):
        try:
            del self._db[key]
        except KeyError:
            pass

        try:
            self._bdb.delete(key)
        except db.DBNotFoundError:
            return defer.succeed(None)

        return defer.succeed(True)

