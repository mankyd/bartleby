from twisted.application import internet, service

import bartleby

# 8007 is the port you want to run under. Choose something >1024
application = service.Application('bartleby')
factory = bartleby.BartlebyFactory()
internet.TCPServer(9007, factory).setServiceParent(service.IServiceCollection(application))

