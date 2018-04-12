#!/usr/bin/env python
########################################################################
# $HeadURL$
# File :    dirac-dms-add-file
# Author :  Stuart Paterson
########################################################################
"""
  Obsoleted
"""
__RCSID__ = "97bf778 (2013-04-05 17:04:01 +0200) FedericoStagni <fstagni@cern.ch>"

from DIRAC.Core.Base import Script

Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                     'Usage:',
                                     '  This is an obsoleted command, use dirac-dms-add-file instead' ] ) )
Script.parseCommandLine( ignoreErrors = True )
args = Script.getPositionalArgs()

from DIRAC import gLogger, exit as DIRACexit

if len( args ) < 3 or len( args ) > 4:
  Script.showHelp()

gLogger.notice( 'This is an obsoleted command, use dirac-dms-add-file instead' )

DIRACexit( 0 )
