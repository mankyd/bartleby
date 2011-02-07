from bsddb import db
import threading
from twisted.internet import defer, error, protocol, reactor
from twisted.python import log

from bartleby.protocol import BartlebyProtocol

class BartlebyFactory(protocol.ServerFactory):
    protocol = BartlebyProtocol

    def __init__(self, config):
        if config['persistence'] not in ('none', 'snapshot', 'everything'):
            raise ValueError('persistence option must be one of the following: "none", "snapshot", or "everything"')

        self.persist = config['persistence'] == 'everything'
        self.persist_snapshot = config['persistence'] == 'snapshot'

        self._db_loc = config['db']
        self._db = {}
        self._dirty = {}
        self._bdb = {}

        self._dirty_lock = None
        self._snapshot_lock = None
        self._snapshot_call = None
        if self.persist_snapshot:
            self._dirty_lock = threading.Lock()
            self._snapshot_lock = threading.Lock()

    def doStart(self):
        if self.persist_snapshot:
            self._snapshot_call = reactor.callLater(1, self._snapshot)

    def doStop(self):
        if self.persist_snapshot:
            self._snapshot_lock.acquire()
            if not self._snapshot_call.active():
                try:
                    self._snapshot_call.cancel()
                except error.AlreadyCalled:
                    pass
            self._snapshot_call = None
            self.__snapshot_run()

        for db_name in self._bdb:
            self._bdb[db_name].close()

    def add_db(self, db_name):
        if not db_name in self._bdb:
            if self.persist or self.persist_snapshot:
                self._bdb[db_name] = db.DB()
                self._bdb[db_name].open(self._db_loc, db_name, db.DB_HASH, db.DB_CREATE | db.DB_THREAD)
        self._db.setdefault(db_name, {})
        self._dirty.setdefault(db_name, {})

    def set(self, db_name, key, val=0):
        if self.persist_snapshot:
            self._dirty_lock.acquire()
            self._db[db_name][key] = val
            self._dirty[db_name][key] = val
            self._dirty_lock.release()
        else:
            self._db[db_name][key] = val
            if self.persist:
                self._bdb[db_name].put(key, str(val))
                self._bdb[db_name].sync()
        return defer.succeed(val)

    def incr(self, db_name, key, val=1):
        if self.persist or self.persist_snapshot:
            return self.set(db_name, key, self._db[db_name].get(key, int(self._bdb[db_name].get(key, 0))) + val)
        else:
            return self.set(db_name, key, self._db[db_name].get(key, 0) + val)

    def add(self, db_name, key, val=0):
        if key in self._db[db_name]:
            return defer.succeed(None)

        if self.persist:
            existing_val = self._bdb[db_name].get(key)
            if existing_val is not None:
                self._db[db_name][key] = int(existing_val)
                return defer.succeed(None)
        
        return self.set(db_name, key, val)

    def replace(self, db_name, key, val=0):
        existing_val = self._db[db_name].get(key)
        if self.persist and existing_val is None:
            existing_val = self._bdb[db_name].get(key)
        if existing_val is None:
            return defer.succeed(None)
            
        return self.set(db_name, key, val)

    def get(self, db_name, keys):
        result = {}
        for key in keys:
            if False and key in self._db[db_name]:
                result[key] = self._db[db_name][key]
            elif self.persist or self.persist_snapshot:
                val = self._bdb[db_name].get(key)
                if val is not None:
                    self._db[db_name][key] = int(val)
                    result[key] = int(val)
        return defer.succeed(result)

    def delete(self, db_name, key):
        try:
            del self._db[db_name][key]
        except KeyError:
            if not self.persist:
                return defer.succeed(True)

        if self.persist:
            try:
                self._bdb[db_name].delete(key)
            except db.DBNotFoundError:
                return defer.succeed(None)

        return defer.succeed(True)

    def _snapshot(self):
        reactor.callInThread(self._snapshot_run)

    def _snapshot_run(self):
        self._snapshot_lock.acquire()
        if self._snapshot_call is None:
            return

        self.__snapshot_run()
        self._snapshot_call = reactor.callLater(10, self._snapshot_run)

        self._snapshot_lock.release()

    def __snapshot_run(self):
        self._dirty_lock.acquire()
        dirty = self._dirty
        self._dirty = {}
        for db_name in dirty:
            self._dirty[db_name] = {}
        self._dirty_lock.release()
        for db_name in dirty:
            for k, v in dirty[db_name].items():
                self._bdb[db_name].put(k, str(v))
            self._bdb[db_name].sync()
