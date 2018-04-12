#!/usr/bin/env python
########################################################################
# $HeadURL$
# File :    dirac-admin-service-ports
# Author :  Stuart Paterson
########################################################################
"""
  Print the service ports for the specified setup
"""
__RCSID__ = "6fa9988 (2010-12-14 13:19:32 +0000) Ricardo Graciani <graciani@ecm.ub.es>"
import DIRAC
from DIRAC.Core.Base import Script

Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                     'Usage:',
                                     '  %s [option|cfgfile] ... [Setup]' % Script.scriptName,
                                     'Arguments:',
                                     '  Setup:    Name of the setup' ] ) )
Script.parseCommandLine( ignoreErrors = True )
args = Script.getPositionalArgs()

setup = ''
if args:
  setup = args[0]

from DIRAC.Interfaces.API.DiracAdmin                         import DiracAdmin
diracAdmin = DiracAdmin()
result = diracAdmin.getServicePorts( setup, printOutput = True )
if result['OK']:
  DIRAC.exit( 0 )
else:
  print result['Message']
  DIRAC.exit( 2 )

