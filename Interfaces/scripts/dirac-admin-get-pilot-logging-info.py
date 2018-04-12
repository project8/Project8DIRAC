#!/usr/bin/env python
########################################################################
# $HeadURL$
# File :    dirac-admin-get-pilot-logging-info.py
# Author :  Stuart Paterson
########################################################################
"""
  Retrieve logging info of a Grid pilot
"""
__RCSID__ = "bfe41d7 (2014-06-16 09:58:26 +0200) FedericoStagni <fstagni@cern.ch>"

from DIRAC.Core.Base import Script

Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                     'Usage:',
                                     '  %s [option|cfgfile] ... PilotID ...' % Script.scriptName,
                                     'Arguments:',
                                     '  PilotID:  Grid ID of the pilot' ] ) )
Script.parseCommandLine( ignoreErrors = True )
args = Script.getPositionalArgs()

if len( args ) < 1:
  Script.showHelp()

from DIRAC import exit as DIRACExit
from DIRAC.Interfaces.API.DiracAdmin                         import DiracAdmin
diracAdmin = DiracAdmin()
exitCode = 0
errorList = []

for gridID in args:

  result = diracAdmin.getPilotLoggingInfo( gridID )
  if not result['OK']:
    errorList.append( ( gridID, result['Message'] ) )
    exitCode = 2
  else:
    print 'Pilot Reference: %s', gridID
    print result['Value']
    print

for error in errorList:
  print "ERROR %s: %s" % error

DIRACExit( exitCode )
