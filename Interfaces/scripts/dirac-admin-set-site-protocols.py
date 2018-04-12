#! /usr/bin/env python
########################################################################
# $HeadURL$
# File :    dirac-admin-set-site-protocols
# Author :  Stuart Paterson
########################################################################
"""
  Defined protocols for each SE for a given site.
"""
__RCSID__ = "5fd3061 (2010-12-14 13:24:40 +0000) Ricardo Graciani <graciani@ecm.ub.es>"
import DIRAC
from DIRAC.Core.Base import Script

Script.registerSwitch( "", "Site=", "Site for which protocols are to be set (mandatory)" )
Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                     'Usage:',
                                     '  %s [option|cfgfile] ... Protocol ...' % Script.scriptName,
                                     'Arguments:',
                                     '  Protocol: SE access protocol (mandatory)' ] ) )
Script.parseCommandLine( ignoreErrors = True )

site = None
for switch in Script.getUnprocessedSwitches():
  if switch[0].lower() == "site":
    site = switch[1]

args = Script.getPositionalArgs()

if not site or not args:
  Script.showHelp()

from DIRAC.Interfaces.API.DiracAdmin                         import DiracAdmin
diracAdmin = DiracAdmin()
exitCode = 0
result = diracAdmin.setSiteProtocols( site, args, printOutput = True )
if not result['OK']:
  print 'ERROR: %s' % result['Message']
  exitCode = 2

DIRAC.exit( exitCode )
