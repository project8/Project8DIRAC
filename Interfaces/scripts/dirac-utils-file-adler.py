#!/usr/bin/env python
########################################################################
# File :    dirac-utils-file-adler
# Author :  
########################################################################
"""
  Calculate alder32 of the supplied file
"""
__RCSID__ = "990980e (2015-03-13 10:57:11 +0100) FedericoStagni <fstagni@cern.ch>"
import DIRAC
from DIRAC.Core.Utilities.Adler     import fileAdler
from DIRAC.Core.Base                import Script

Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                     'Usage:',
                                     '  %s [option|cfgfile] ... File ...' % Script.scriptName,
                                     'Arguments:',
                                     '  File:     File Name' ] ) )
Script.parseCommandLine( ignoreErrors = False )
files = Script.getPositionalArgs()
if len( files ) == 0:
  Script.showHelp()

exitCode = 0

for fa in files:
  adler = fileAdler( fa )
  if adler:
    print fa.rjust( 100 ), adler.ljust( 10 )
  else:
    print 'ERROR %s: Failed to get adler' % fa
    exitCode = 2

DIRAC.exit( exitCode )
