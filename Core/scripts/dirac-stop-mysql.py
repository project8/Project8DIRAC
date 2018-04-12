#!/usr/bin/env python
########################################################################
# File :   dirac-stop-mysql
# Author : Ricardo Graciani
########################################################################
"""
  Stop DIRAC MySQL server
"""
__RCSID__ = "393aa29 (2016-03-03 17:33:17 +0100) FedericoStagni <federico.stagni@cern.ch>"
#
from DIRAC.Core.Base import Script
Script.disableCS()
Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                     'Usage:',
                                     '  %s [option|cfgfile] ...' % Script.scriptName,
                                     ] ) )
Script.parseCommandLine()
#
from DIRAC.FrameworkSystem.Client.ComponentInstaller import gComponentInstaller
#
gComponentInstaller.exitOnError = True
#
print gComponentInstaller.stopMySQL()['Value'][1]
