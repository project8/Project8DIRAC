#!/usr/bin/env python
########################################################################
# $HeadURL$
# File :    dirac-install-mysql
# Author :  Ricardo Graciani
########################################################################
"""
Do the initial installation and configuration of the DIRAC MySQL server
"""
__RCSID__ = "6ebd9cd (2016-01-06 13:30:10 +0100) Sbalbp <sbalbp@gmail.com>"

from DIRAC.Core.Base import Script

Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1] ] ) )

Script.parseCommandLine()


#
from DIRAC.FrameworkSystem.Client.ComponentInstaller import gComponentInstaller
#
gComponentInstaller.exitOnError = True
#
gComponentInstaller.getMySQLPasswords()
#
gComponentInstaller.installMySQL()
#
gComponentInstaller._addMySQLToDiracCfg()
