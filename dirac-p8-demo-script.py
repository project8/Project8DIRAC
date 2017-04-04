#!/usr/bin/env python
'''
    Example of cleaned up implementation
'''

from p8_base_script import BaseScript
from DIRAC import S_OK, S_ERROR, gLogger, exit as DIRACExit
#from DIRAC.Core.Base import Script
#Script.parseCommandLine()

__RCSID__ = '$Id$'

class DemoScript(BaseScript):
    def main(self):#, *args, **kwargs):
        if not len(self.args):
            gLogger.error("nothing to say")
            DIRACExit(1)
        for tosay in self.args:
            #if getattr(self, 'upper', False):
            if self.show_raw:
                gLogger.notice(tosay)
            if self.upper:
                tosay = tosay.upper()
            gLogger.notice(tosay)

        gLogger.notice('Done!')

if __name__ == "__main__":
    script = DemoScript([
                         ('u', 'upper', 'Print in upper case'),
                         ('r', 'show-raw', 'Show raw result from the query'),
                         ('p:', 'num-pings=', 'Number of pings to do (default 1)', 1),
                        ])
    script()
