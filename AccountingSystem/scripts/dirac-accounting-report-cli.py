#!/usr/bin/env python
########################################################################
# $HeadURL$
# File :   dirac-accounting-report-cli
# Author : Adria Casajus
########################################################################
"""
  Command line interface to DIRAC Accounting ReportGenerator Service.
"""
__RCSID__ = "a6f71fd (2016-02-04 16:11:59 +0100) Andrei Tsaregorodtsev <atsareg@in2p3.fr>"

from DIRAC.Core.Base import Script

Script.localCfg.addDefaultEntry( "LogLevel", "info" )
Script.setUsageMessage('\n'.join( [ __doc__.split( '\n' )[1],
                                    'Usage:',
                                    '  %s [option|cfgfile] ...' % Script.scriptName, ] )   )
Script.parseCommandLine()

from DIRAC.AccountingSystem.Client.ReportCLI import ReportCLI

if __name__=="__main__":
    reli = ReportCLI()
    reli.start()
