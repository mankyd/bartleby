# bartleby/tap.py
from twisted.application import internet, service
from twisted.internet import interfaces
from twisted.python import usage

import sys


from bartleby import BartlebyFactory

class Options(usage.Options):

    optParameters = [
        ["port", "p", 7009, 'Port to listen on', int],
        ]

def makeService(config):
    ser = service.MultiService()
    factory = BartlebyFactory()
    internet.TCPServer(config['port'], factory).setServiceParent(ser)

    return ser
