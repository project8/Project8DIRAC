#!/usr/bin/env python
'''
    Example of cleaned up implementation
'''

from p8_base_script import BaseScript
#from DIRAC import S_OK, S_ERROR, gLogger, exit as DIRACExit
#from DIRAC.Core.Base import Script
#Script.parseCommandLine()

__RCSID__ = '$Id$'

class DemoScript(BaseScript):
    def num_pings(self, value):
        self._num_pings = value
        return S_OK()

if __name__ == "__main__":
