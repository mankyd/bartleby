from twisted.application import internet, service
from twisted.internet import defer, protocol, reactor
from twisted.protocols import basic
from twisted.python import log

MAX_KEY_LENGTH = 255

class ProtocolError(Exception):
    def __init__(self, k, msg=None):
        self.k = k
        self.msg = msg

    def __str__(self):
        if self.msg is not None:
            return "%s %s" % (self.k, self.msg)
        return self.k

class ClientError(ProtocolError):
    def __init__(self, msg):
        ProtocolError.__init__(self, 'CLIENT_ERROR', msg)

class ServerError(ProtocolError):
    def __init__(self, msg):
        ProtocolError.__init__(self, 'SERVER_ERROR', msg)

class MaxKeyLenError(ClientError):
    def __init__(self):
        ClientError.__init__(self, 'key length exceeded MAX_KEY_LENGTH of %d characters' % MAX_KEY_LENGTH)


class BartlebyProtocol(basic.LineReceiver):
    def __init__(self, *args, **kwargs):
        self.raw_mode = False
        self.raw_key = None
        self.raw_bytes = 0
        self.raw_reply = True
        self.data_buff = ''

    def lineReceived(self, data):
        log.msg(data)
        reply = True
        d = None
        try:
            if data.startswith('incr '):
                key, val, reply = self._parse_incr_decr(data[5:])
                d = self.factory.incr(key, val)
                d.addCallback(self._success_incr_decr, reply=reply)

            elif data.startswith('decr '):
                key, val, reply = self._parse_incr_decr(data[5:])
                d = self.factory.decr(key, val)
                d.addCallback(self._success_incr_decr, reply=reply)

            elif data.startswith('get '):
                keys = self._parse_get(data[4:])
                d = self.factory.get(keys)
                d.addCallback(self._success_get)
            elif data.startswith('gets '):
                keys = self._parse_get(data[5:])
                d = self.factory.get(keys)
                d.addCallback(self._success_gets)
            elif data.startswith('set '):
                key, bytes, reply = self._parse_set(data[4:])
                if bytes == 0:
                    d = self.factory.set(key, 0)
                    d.addCallback(self._success_set, reply=reply)
                else:
                    self.raw_mode = 'set'
                    self.raw_key = key
                    self.raw_bytes = bytes
                    self.raw_reply = reply
                    self.setRawMode()
            elif data.startswith('add '):
                key, bytes, reply = self._parse_set(data[4:])
                if bytes == 0:
                    d = self.factory.add(key, 0)
                    d.addCallback(self._success_set, reply=reply)
                else:
                    self.raw_mode = 'add'
                    self.raw_key = key
                    self.raw_bytes = bytes
                    self.raw_reply = reply
                    self.setRawMode()

            elif data.startswith('replace '):
                key, bytes, reply = self._parse_set(data[8:])
                if bytes == 0:
                    d = self.factory.replace(key, 0)
                    d.addCallback(self._success_set, reply=reply)
                else:
                    self.raw_mode = 'replace'
                    self.raw_key = key
                    self.raw_bytes = bytes
                    self.raw_reply = reply
                    self.setRawMode()

            elif data.startswith('delete '):
                key, reply = self._parse_delete(data[7:])
                d = self.factory.delete(key)
                d.addCallback(self._success_delete, reply=reply)

            else:
                raise ProtocolError('ERROR')
        except ProtocolError, e:
            self.send_response(e)

    def rawDataReceived(self, data):
        if self.raw_mode:
            try:
                self.data_buff += data
                if len(self.data_buff) >= self.raw_bytes:
                    try:
                        val = int(self.data_buff[:self.raw_bytes])
                        self.data_buff = self.data_buff[self.raw_bytes:]
                    except ValueError:
                        self.raw_mode = False
                        self.data_buff = self.data_buff[self.raw_bytes:]
                        raise ClientError('Can only set integers')

                    if not self.data_buff.startswith('\r\n'):
                        self.raw_mode = False
                        raise ClientError('Too many bytes sent for set')

                    if self.raw_mode == 'set':
                        d = self.factory.set(self.raw_key, val)
                    elif self.raw_mode == 'add':
                        d = self.factory.add(self.raw_key, val)
                    elif self.raw_mode == 'replace':
                        d = self.factory.replace(self.raw_key, val)
                    d.addCallback(self._success_set, reply=self.raw_reply)

                    self.raw_mode = False
                    self.setLineMode(self.data_buff[2:])
                    self.data_buff = ''
            except ProtocolError, e:
                self.send_response(e)
                if self.data_buff.startswith('\r\n'):
                    self.setLineMode(self.data_buff[2:])
                else:
                    self.setLineMode(self.data_buff)
        else:
            self.setLineMode(self.data_buff + data)
            self.data_buff = ''
            
    def send_response(self, msg):
        self.transport.write("%s\r\n" % msg)

    @classmethod
    def _parse_incr_decr(cls, data):
        key = ''
        val = 0
        reply = True
        negative = False

        pos = data.find(' ')
        if pos > MAX_KEY_LENGTH:
            raise MaxKeyLenError()
        else:
            if pos == -1:
                key = data
                val = 1
            else:
                key = data[:pos]
                pos += 1
                if data[pos] == '-':
                    negative = True
                    pos += 1
                num_start = pos
                try:
                    while True:
                        val = val * 10 + int(data[pos])
                        pos += 1
                except:
                    if pos == num_start:
                        if negative:
                            raise ClientError('invalid value format. expected integer after "-"')
                        val = 1
                    else:
                        pos += 1

                if data.find('noreply', pos) == pos:
                    reply = False

        if negative:
            return (key, -val, reply)
        return (key, val, reply)

    @classmethod
    def _parse_get(cls, data):
        return filter(None, data.split(' '))

    def _parse_set(cls, data):
        #<command name> <key> <flags> <exptime> <bytes> [noreply]\r\n
        tokens = data.split(' ')
        reply = True
        if len(tokens) == 1:
            key = tokens[0]
            bytes = 0
        elif len(tokens) == 2:
            key = tokens[0]
            bytes = int(tokens[1])
        elif len(tokens) == 3 and tokens[2] == 'noreply':
            key = tokens[0]
            bytes = int(tokens[1])
            reply = False
        elif len(tokens) == 4:
            key = tokens[0]
            bytes = int(tokens[3])
        elif len(tokens) == 5:
            key = tokens[0]
            bytes = int(tokens[3])
            reply = False
        else:
            raise ClientError('Did not understand request')

        if len(key) == 0:
            raise ClientError('No key specified')
        elif len(key) > MAX_KEY_LENGTH:
            raise MaxKeyLenError()
        elif bytes < 0:
            raise ClientError('Can not specified a negative byte length')

        return (key, bytes, reply)

    @classmethod
    def _parse_delete(self, data):
        tokens = data.split(' ')
        reply = True
        if len(tokens) == 0 or len(tokens) > 2:
            raise ClientError('Malformed delete request')

        key = tokens[0]
        if len(tokens) == 2:
            if tokens[1] != 'noreply':
                raise ClientError('Malformed delete request')
            reply = False

        if len(key) > MAX_KEY_LENGTH:
            raise MaxKeyLenError()

        return key, reply

    def _success_incr_decr(self, result, reply=True, *args, **kwargs):
        if reply:
            self.send_response(result)

    def _success_get(self, result):
        for k, v in result.items():
            num = str(v)
            self.send_response('VALUE %s 0 %d' % (k, len(num)))
            self.send_response(num)
        self.send_response('END')

    def _success_gets(self, result):
        for k, v in result.items():
            num = str(v)
            self.send_response('VALUE %s 0 %d 0' % (k, len(num)))
            self.send_response(num)
        self.send_response('END')

    def _success_set(self, result, reply=True):
        if reply:
            if result is None:
                self.send_response('NOT_STORED')
            else:
                self.send_response('STORED')

    def _success_delete(self, result, reply=True):
        if reply:
            if result is None:
                self.send_response('NOT_FOUND')
            else:
                self.send_response('DELETED')
        
class BartlebyFactory(protocol.ServerFactory):
    protocol = BartlebyProtocol

    def __init__(self):
        self._db = {}

    def incr(self, key, val=1):
        self._db[key] = self._db.get(key, 0) + val
        return defer.succeed(str(self._db[key]))

    def _incr(self, key, val):
        pass

    def decr(self, key, val=1):
        self._db[key] = self._db.get(key, 0) - val
        return defer.succeed(str(self._db[key]))

    def set(self, key, val=0):
        self._db[key] = val
        return defer.succeed(str(val))

    def add(self, key, val=0):
        if key in self._db:
            return defer.succeed(None)
        return self.set(key, val)

    def replace(self, key, val=0):
        if not key in self._db:
            return defer.succeed(None)
        return self.set(key, val)

    def get(self, keys):
        result = {}
        for key in keys:
            if key in self._db:
                result[key] = self._db[key]
        return defer.succeed(result)

    def delete(self, key):
        if not key in self._db:
            return defer.succeed(None)
        del self._db[key]
        return defer.succeed(True)

# 8007 is the port you want to run under. Choose something >1024
application = service.Application('bartleby')
factory = BartlebyFactory()
internet.TCPServer(9007, factory).setServiceParent(
    service.IServiceCollection(application))

