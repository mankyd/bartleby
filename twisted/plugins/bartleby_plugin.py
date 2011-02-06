from twisted.application.service import ServiceMaker

bartleby = ServiceMaker(
    'bartleby', 'bartleby.tap', 'Run a bartleby server', 'bartleby')
