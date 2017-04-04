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

        Note that in the implementation of self.main() any configured switches show 
            up in kwargs, positional arguments are in args
        '''
        for switch in switches:
            self.registerSwitch(switch)
        Script.parseCommandLine(ignoreErrors=False)
        self.args = Script.getPositionalArgs()

    def __call__(self):
        gLogger.info('time to call main()')
        if hasattr(self, 'main'):
            self.main()
        else:
            gLogger.error('no main method implemented')
            DIRACExit(1)
        DIRACExit(0)

    def _default_set(self, name, value):
        gLogger.info('calling set {} -> {}'.format(name,repr(value)))
        # This is so ugly, they set to '' when a flag is there with no value
        #   ... '' is logical False in python! grrrr (BHL)
        if value is '':
            value = True
        setattr(self, name, value)
        return S_OK()

    def registerSwitch(self, switch):
        this_name = switch[1].replace('-', '_').rstrip(':=')

        # if there isn't a setter method, use default
        if not hasattr(self, 'set_'+this_name):
            gLogger.debug('configuring default setter for <{}>'.format(this_name))
            setattr(self, 'set_'+this_name, lambda x: self._default_set(this_name, x))
        try:
            setter = getattr(self, 'set_'+this_name)
            getattr(self, 'set_'+this_name)(None)
        except:
            gLogger.error('unable to call {}'.format('set_'+this_name))
            pass

        Script.registerSwitch(switch[0], switch[1], switch[2], getattr(self, 'set_'+this_name))

        # if a default was provided, set it
        if len(switch) == 4:
            getattr(self, 'set_'+this_name)(switch[3])
