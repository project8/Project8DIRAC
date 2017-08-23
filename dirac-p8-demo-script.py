#!/usr/bin/env python
'''
    Example of cleaned up implementation
'''

from p8_base_script import BaseScript
#from DIRAC.Core.Base import Script
#Script.parseCommandLine()

__RCSID__ = '$Id$'

class DemoScript(BaseScript):
    # the doc string goes into the CLI --help
    '''
        Demo of creating your own class using the P8 BaseScript class
    '''
    # switches is a *class attribute* which defines commandline flags
    # these must follow the method signature from DIRAC.Core.Base.Script.registerSwitch
    switches = [
                ('u', 'upper', 'Print in upper case'),
                ('r', 'show-raw', 'Show raw result from the query'),
                ('p:', 'num-pings=', 'Number of pings to do (default 1)', 1),
               ]

    # whatever you actually want to do goes in the main() method.
    def main(self):
        # your dirac imports must go here if you need any
        from DIRAC import S_OK, S_ERROR, gLogger, exit as DIRACExit

        # and then logic
        if not len(self.args):
            gLogger.error("nothing to say")
            DIRACExit(1)
        for tosay in self.args:
            #if getattr(self, 'upper', False)
            gLogger.info("you asked me to say:")
            if self.show_raw:
                gLogger.notice(tosay)
            if self.upper:
                tosay = tosay.upper()
            gLogger.notice(tosay)

        gLogger.notice('Done doing demo action!')

# abstracting this would be a pain, just copy/past it and update the class name
if __name__ == "__main__":
    script = DemoScript()
    script()
