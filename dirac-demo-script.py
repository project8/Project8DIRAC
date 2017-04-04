#!/usr/bin/env python

'''
    dirac-demo-script

        This script prints out how great it is, shows raw queries and sets the number of pings.

        Usage:
            dirac-demo-script [option|cfgfile] <Args>
        Arguments:
            <service1> [<service2> ...]
'''

from DIRAC import S_OK, S_ERROR, gLogger, exit as DIRACExit
from DIRAC.Core.Base import Script

__RCSID__ = '$Id$'

class Params:
    '''
        Class holding parameters... because we can't use arg parse
    '''

    def __init__(self):
        self.raw = False
        self.pingsToDo = 1
        self.registerSwitches()

    def setRawResult(self, value):
        self.raw = True
        return S_OK()

    def setNumOfPingsToDo(self, value):
        try:
            gLogger.info("ping value: {}".format(value))
            self.pingsToDo = max(1, int(value))
        except ValueError:
            gLogger.error("    recieved {}".format(value))
            return S_ERROR("Number of pings must be a number")
        return S_OK()

    def registerSwitches(self):
        '''
            Register all cli switches that may be used while calling the script
        '''

        switches = [
                    ('', 'text=', 'Text to print'),
                    ('u', 'upper', 'Print text in upper case'),
                    ('r', 'showRaw', 'Show raw result from the query', self.setRawResult),
                    ('p:', 'numPings=', 'Number of pings to do (default 1)', self.setNumOfPingsToDo),
                   ]

        for switch in switches:
            Script.registerSwitch(*switch)

        Script.setUsageMessage(__doc__)

    def parseSwitches(self):
        '''
            Parse switches and positional args
        '''
        Script.parseCommandLine(ignoreErrors=False)

        servicesList = Script.getPositionalArgs()

        gLogger.info('This is the servicesList: {}'.format(servicesList))

        switches = dict(Script.getUnprocessedSwitches())

        gLogger.debug("the switches used are: {}")
        map(gLogger.debug, switches.iteritems())

        switches['servicesList'] = servicesList

        return switches

def main(switchDict):
    '''
        Main method for the script, has the actual logic
    '''

    if not len(switchDict['servicesList']):
        gLogger.error('No services defined')
        DIRACExit(1)
    gLogger.notice("We are done")

if __name__ == '__main__':
    params = Params()
    switchDict = params.parseSwitches()

    from DIRAC.Interfaces.API.Dirac import Dirac

    main(switchDict)

    DIRACExit(0)
