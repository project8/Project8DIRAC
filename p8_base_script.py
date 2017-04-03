'''
    Base class to clean up implementing scripts in DIRAC
'''

from DIRAC import S_OK, S_ERROR, gLogger, exit as DIRAC_Exit
from DIRAC.Core.Base import Script

class BaseScript(object):

    def __init__(self, switches=[]):
        '''
        switches is a list of 3 or 4 element tuples, where the elements are:
             0 : short form (single character) command line flag
             1 : long form (- separated words) command line flag
             2 : description string shown with --help
            [3]: default value to store once the item is registered

            By default, data for each flag are stored in self._<long>.replace('-','_')
            The value for each is set by the method self.set_<long>.replace('-','_'),
            a method which performs no logic by default but which may be overridden.
        '''
        for switch in switches:
            self.registerSwitch(switch)
        Script.parseCommandLine()

    def __call__(self, *args, **kwargs):
        if hasattr(self, 'main'):
            self.main(*args, **kwargs)
        else:
            gLogger.error('no main method implemented')
            DIRACExit(1)

    def registerSwitch(self, switch):
        this_name = '_' + switch[1].replace('-', '_')

        # if there isn't a setter method, use default
        if not hasattr(self, 'set_'+this_name):
            setattr(self, 'set'+this_name, lambda x: setattr(self, this_name, x))

        Script.registerSwitch(*switch[0:3], getattr(self, 'set'+this_name))

        # if a default was provided, set it
        if len(switch) == 4:
            getattr(self, 'set'+this_item)(switch[3])

    def DiracImports(self):
        # can't import DIRAC stuff until after we parse the commandline,
        # but parsing the commandline needs to be after we register our switches
