#!/usr/bin/env python
"""
  Restart DIRAC component using runsvctrl utility
"""
__RCSID__ = "78cc21a (2016-04-22 12:27:17 +0200) Federico Stagni <federico.stagni@cern.ch>"
#
from DIRAC.Core.Base import Script
Script.disableCS()
Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                     'Usage:',
                                     '  %s [option|cfgfile] ... [System [Service|Agent]]' % Script.scriptName,
                                     'Arguments:',
                                     '  System:        Name of the system for the component (default *: all)',
                                     '  Service|Agent: Name of the particular component (default *: all)' ] ) )
Script.parseCommandLine()
args = Script.getPositionalArgs()
if len( args ) > 2:
  Script.showHelp()
  exit( -1 )

system = '*'
component = '*'
if len( args ) > 0:
  system = args[0]
if system != '*':
  if len( args ) > 1:
    component = args[1]
#
from DIRAC.FrameworkSystem.Client.ComponentInstaller import gComponentInstaller
#
gComponentInstaller.exitOnError = True
#
result = gComponentInstaller.runsvctrlComponent( system, component, 't' )
if not result['OK']:
  print 'ERROR:', result['Message']
  exit( -1 )

gComponentInstaller.printStartupStatus( result['Value'] )
