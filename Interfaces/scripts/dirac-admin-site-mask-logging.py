#!/usr/bin/env python
########################################################################
# $HeadURL$
# File :    dirac-admin-site-mask-logging
# Author :  Stuart Paterson
########################################################################
"""
  Retrieves site mask logging information.
"""
__RCSID__ = "345d6fc (2010-12-14 13:26:14 +0000) Ricardo Graciani <graciani@ecm.ub.es>"
import DIRAC
from DIRAC.Core.Base import Script

Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                     'Usage:',
                                     '  %s [option|cfgfile] ... Site ...' % Script.scriptName,
                                     'Arguments:',
                                     '  Site:     Name of the Site' ] ) )

Script.parseCommandLine( ignoreErrors = True )
args = Script.getPositionalArgs()

if len( args ) < 1:
  Script.showHelp()

from DIRAC.Interfaces.API.DiracAdmin                         import DiracAdmin
diracAdmin = DiracAdmin()
exitCode = 0
errorList = []

for site in args:
  result = diracAdmin.getSiteMaskLogging( site, printOutput = True )
  if not result['OK']:
    errorList.append( ( site, result['Message'] ) )
    exitCode = 2

for error in errorList:
  print "ERROR %s: %s" % error

DIRAC.exit( exitCode )
