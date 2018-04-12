#!/usr/bin/env python
########################################################################
# $HeadURL$
# File :   dirac-admin-accounting-cli
# Author : Adria Casajus
########################################################################
"""
  Command line administrative interface to DIRAC Accounting DataStore Service
"""
__RCSID__ = "a6f71fd (2016-02-04 16:11:59 +0100) Andrei Tsaregorodtsev <atsareg@in2p3.fr>"

from DIRAC.Core.Base import Script

Script.localCfg.addDefaultEntry( "LogLevel", "info" )
Script.setUsageMessage('\n'.join( [ __doc__.split( '\n' )[1],
                                    'Usage:',
                                    '  %s [option|cfgfile] ...' % Script.scriptName, ] )   )
Script.parseCommandLine()

from DIRAC.AccountingSystem.Client.AccountingCLI import AccountingCLI

if __name__=="__main__":
    acli = AccountingCLI()
    acli.start()