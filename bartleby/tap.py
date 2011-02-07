# bartleby/tap.py
from twisted.application import internet, service
from twisted.internet import interfaces
from twisted.python import usage

import sys


from bartleby import BartlebyFactory

class Options(usage.Options):

    optParameters = [
        ["port", "p", 7009, 'Port to listen on', int],
        ['persistence', 's', 'none', 'Level of persistence required. Options are "none", "snapshot", and "everything"'],
        ['db', 'd', 'test.db', 'Location of database. Not used if persistence level is "none"'],
        ]

def makeService(config):
    ser = service.MultiService()
    factory = BartlebyFactory(config)
    internet.TCPServer(config['port'], factory).setServiceParent(ser)

    return ser
